# 第 3 章 Agent 循环（The Loop）

> 上一章：[第 2 章 消息与上下文管理](02-消息与上下文管理.md) ｜ 下一章：[第 4 章 工具系统](04-工具系统.md)

---

## 一、概念：循环是 Harness 的心脏

第 0 章画了核心循环的五步。本章把它展开成一个真正的**状态机**，并回答那些第 0 章遗留的问题：多工具调用、超时、取消、重试、防死循环。

Agent 循环的本质是：**「调模型 → 拿工具调用 → 执行 → 回填 → 再调」的持续过程，直到模型不再请求工具，或到达某个终止条件。**

---

## 二、原理：循环 = 状态机 + 策略钩子

一个好的循环设计，把「**主流程**」和「**策略**」分开：

- **主流程**（状态机）：固定的「调用 → 解析 → 执行 → 回填」骨架。
- **策略**（钩子）：超时、重试、取消、防重复……这些是**可变**的，应该通过钩子注入，而不是写死在主流程里。

为什么？因为策略会随场景变化：CI 里可能要「失败就重试 3 次」，交互模式可能要「卡住就交给用户决定」。如果把策略写死，循环就无法复用。

---

## 三、pi 的实现：事件流驱动的循环

pi 在 `pi-agent-core` 里实现循环，核心特征是**事件流**。`prompt()` 不返回最终结果，而是广播一串事件：

```
prompt("Hello")
├─ agent_start
├─ turn_start
├─ message_start   { message: userMessage }      // 用户输入
├─ message_end
├─ message_start   { message: assistantMessage } // 模型开始回
├─ message_update  { message: partial... }       // 流式片段
├─ toolcall_start  { contentIndex: 0 }
├─ toolcall_delta  { partial: 部分JSON }
├─ toolcall_end    { toolCall: {...} }
├─ （工具执行，结果回填，继续下一轮 turn）
├─ message_start → message_update → ...
└─ done            { reason: "stop" }
```

**关键点**：`turn` 是「一次模型调用 + 其后的工具执行」，一次 `prompt()` 可能包含多个 `turn`。事件流让每一步都可被 UI 订阅、可被观测、可被中断。

---

## 四、DSH 的实现：循环策略插件化

DSH 把循环拆成多个插件，循环中的策略变成独立的包：

- `dsh-agent` + `dsh-agent-loop`：循环本体。
- `dsh-tool-call-timeout-policy`：工具调用超时策略。
- `dsh-repeat-tool-reminder`：检测模型反复调用同一工具，提醒它别再重复（防死循环）。
- `dsh-timeout`：通用超时基础设施。

**DSH 的启示**：循环里那些「看似边角」的策略（超时、防重复），正是生产环境里最痛的点。把它们独立成插件，就能单独测试、单独替换。

---

## 五、我们如何设计：带钩子的循环

```ts
interface LoopHooks {
  beforeTool?: (call: ToolCall) => Promise<void>          // 权限门（第 7 章）
  onToolTimeout?: (call: ToolCall) => void
  shouldContinue?: (turnCount: number) => boolean          // 防死循环：最多 N 轮
}

class Agent {
  constructor(
    private model: Model,
    private tools: ToolRegistry,
    private ctx: ContextManager,
    private hooks: LoopHooks = {},
  ) {}

  async *run(userInput: string, history: AgentMessage[]): AsyncGenerator<AgentEvent> {
    yield { type: 'agent_start' }

    const messages: AgentMessage[] = [...history]
    messages.push(this.newMessage('user', userInput, messages.at(-1)?.id ?? null))

    let turn = 0
    while (true) {
      // 防死循环：轮数上限
      if (!(this.hooks.shouldContinue ?? (() => turn < 50))(turn)) {
        yield { type: 'error', error: new Error('max turns reached') }
        return
      }
      turn++
      yield { type: 'turn_start' }

      // ① 装配上下文 + ② 调模型（流式聚合）
      const llmInput = this.ctx.prepare(messages, 'You are a helpful agent.')
      const toolDefs = this.tools.list()

      let text = ''
      const toolCalls: ToolCall[] = []
      for await (const ev of this.model.stream({ messages: llmInput, tools: toolDefs })) {
        if (ev.type === 'text_delta') { text += ev.delta; yield { type: 'text_delta', delta: ev.delta } }
        if (ev.type === 'toolcall_delta') toolCalls.push(ev.call)
        if (ev.type === 'error') { yield { type: 'error', error: ev.error }; return }
      }

      // ③ 无工具调用 → 结束
      if (toolCalls.length === 0) {
        messages.push(this.newMessage('assistant', text, messages.at(-1)!.id))
        yield { type: 'text_end', text }
        yield { type: 'done', reason: 'stop' }
        return
      }

      // ④⑤ 回填 assistant 的 toolCalls，逐个执行工具并回填结果
      messages.push({ ...this.newMessage('assistant', text, messages.at(-1)!.id), toolCalls })
      for (const call of toolCalls) {
        yield { type: 'toolcall_start', call }
        await this.hooks.beforeTool?.(call)                 // 权限检查
        const result = await this.executeWithTimeout(call)   // 带超时
        messages.push(this.newMessage('tool', JSON.stringify(result), messages.at(-1)!.id, call.id))
        yield { type: 'toolcall_end', call, result }
      }
      // 回到 while 继续下一轮
    }
  }

  private async executeWithTimeout(call: ToolCall) {
    return this.tools.run(call.name, call.arguments)   // 简化：超时逻辑可挂在 hooks
  }

  private newMessage(role: string, content: string, parentId: string | null, toolCallId?: string): AgentMessage {
    return { id: uid(), parentId, role, content, timestamp: Date.now(), toolCallId }
  }
}
```

**注意几个关键决策**：

1. **`AsyncGenerator<AgentEvent>`**：循环本身就是一个事件流，UI 直接 `for await` 消费。
2. **多工具调用逐个执行**：`for (const call of toolCalls)`。这里选择「串行」，是多数实现的选择（工具结果互相依赖、副作用有序）。并行是可选优化，但会让回填和错误处理复杂化。
3. **回填顺序必须正确**：先 push assistant 的 `toolCalls`，再 push 对应的 `tool` 结果，且 `toolCallId` 要对应上（第 0 章练习 3 的答案）。
4. **防死循环靠 `shouldContinue`**：一个简单的轮数上限，就是 DSH `repeat-tool-reminder` 的朴素版。

---

## 六、设计原因与考虑维度

**为什么循环必须是「事件流」而不是「返回最终字符串」？** 三个原因（呼应第 0 章）：

1. **流式体验**：用户要看到过程，不是等 30 秒。
2. **UI 解耦**：终端、浏览器、headless 都是订阅者（第 8 章）。
3. **可观测、可中断**：每一步都是事件，就能打点、能取消（第 9 章）。

**设计循环要考虑的维度**：

| 维度 | 要回答的问题 |
|---|---|
| 终止条件 | 文本结束 / 错误 / 轮数上限 / 用户取消 |
| 多工具 | 串行还是并行？一个失败是否中断其余？ |
| 超时 | 工具卡死、模型卡死分别怎么办 |
| 取消 | 用户停止时，正在执行的工具怎么收尾 |
| 重试 | 哪些错误自动重试、哪些抛给用户 |
| 防死循环 | 反复调同一工具、轮数爆炸，如何限制 |
| 回填格式 | assistant 的 toolCalls 和 tool 结果如何对应 |

---

## 七、达成什么目标

读完本章，你应该能：

- ✅ 画出 agent 循环的状态机
- ✅ 解释「主流程」和「策略钩子」为什么要分离
- ✅ 说明 pi 的事件流循环和 DSH 的策略插件化
- ✅ 写出带轮数上限、权限钩子、事件流的完整循环
- ✅ 回答「多工具串行/并行、回填顺序」等细节问题

---

## 八、实操练习

1. **补全超时逻辑**：给 `executeWithTimeout` 加一个 10 秒超时，超时则触发 `hooks.onToolTimeout` 并返回一个「超时」结果。
2. **防重复调用**：记录每个工具最近一次调用的参数，如果模型连续 3 次用相同参数调同一工具，就打断循环并提示。
3. **思考题**：如果第 2 个工具执行失败，第 3 个还执行吗？如果不执行，你如何把「失败」告诉模型，让它重新决策？把答案写下来。

---

> 下一章：[第 4 章 工具系统](04-工具系统.md) —— schema 决定「怎么调」，执行决定「做什么」。
