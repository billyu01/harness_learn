# 第 3 章 Agent 循环（The Loop）

> 上一章：[第 2 章 消息与上下文管理](02-消息与上下文管理.md) ｜ 下一章：[第 4 章 工具系统](04-工具系统.md)

---

## 一、概念：循环是 Harness 的心脏

核心循环是 Harness 的心脏：一个「调模型 → 拿工具调用 → 执行 → 回填 → 再调」的持续过程，直到模型不再请求工具，或到达某个终止条件。

本章把它展开成一个真正的**状态机**，并回答那些关键问题：多工具调用、超时、取消、重试、防死循环。

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
3. **回填顺序必须正确**：先 push assistant 的 `toolCalls`，再 push 对应的 `tool` 结果，且 `toolCallId` 要对应上（最容易写错、也最隐蔽的点）。
4. **防死循环靠 `shouldContinue`**：一个简单的轮数上限，就是 DSH `repeat-tool-reminder` 的朴素版。

---

## 六、方案设计

### 6.1 验收标准

循环「做好」有三条可验证的标准：

1. **收敛**：任何输入都在有限轮内结束（文本 / 报错 / 取消），绝不空转。
2. **顺序正确**：工具结果严格对应它所属的 `toolCallId`，模型看到的上下文是线性、无歧义的。
3. **策略可替换**：超时、重试、防循环这些策略，能不改主流程就替换。

三条标准里，「收敛」是底线，「策略可替换」是长期可维护的关键，而「顺序正确」最容易写错、又最隐蔽。本章重点展开后两条。

### 6.2 关键取舍一：主流程与策略分离（钩子化）

这是循环设计的根本决策。看两种写法：

- **写死**：超时、重试、防循环的逻辑直接散在 `while` 循环里。
- **钩子化**：主流程只保留「调用 → 解析 → 执行 → 回填」，策略通过 `LoopHooks` 注入。

**立场：钩子化，哪怕你暂时只有一个策略。** 理由：策略是最容易随场景变的——CI 里要「失败重试 3 次」，交互模式要「卡住就抛给用户」。如果写死，每换一个场景就改一次核心循环，改着改着就烂了。钩子化的成本极低（就是把几处 `if` 换成 `hooks.xxx?.()`），收益是循环长期稳定。

**失效条件**：一次性脚本、永远不会换场景，写死更直接。但「永远不会换场景」这个前提，在 agent 开发里几乎不成立。

### 6.3 关键取舍二：工具回填顺序（最隐蔽的坑）

一次模型返回多个工具调用时，回填顺序必须严格：

```
正确：
assistant(toolCalls=[A, B]) → tool(A的结果, toolCallId=A) → tool(B的结果, toolCallId=B)

错误：
assistant(toolCalls=[A, B]) → tool(B的结果) → tool(A的结果)   // 顺序错乱
```

**立场：串行执行、按返回顺序回填，`toolCallId` 一一对应。** 理由：LLM 对上下文的顺序极度敏感，顺序错乱会让它在下一轮「误解」哪条结果对应哪个调用，产出错误决策。这是一个**必须保证的不变量**，不是可选优化。

**失效条件**：并行执行时，回填顺序可以「按完成顺序」，但**每个 tool 结果仍必须带上自己的 `toolCallId`**，让模型能通过 id 对齐，而不是靠位置对齐。也就是说，即便并行，id 也不能省。

### 6.4 关键取舍三：收敛的三道防线（先后顺序）

这里给出三道防线的**明确落地顺序**和理由：

1. **轮数上限**（先做）——一行代码，立刻保证「不会无限空转」，是「收敛」的最低保障。
2. **单步超时**（第二）——防止「一次工具调用卡 10 分钟」把整个会话拖死。
3. **重复检测**（最后）——记录「工具 + 参数」历史，连续相同调用则打断。这是语义层面的防循环，复杂度最高。

**立场：按「收益/成本」排序，不要一上来就做重复检测。** 重复检测需要维护调用历史、做参数比较，还可能误伤「合法的重复调用」（比如「再试一次这个命令」）。在轮数上限和超时还没做之前，重复检测是「用高复杂度解决一个低概率问题」。

**失效条件**：如果你的模型特别容易陷入重复调用（某些模型确实如此），重复检测的优先级可以提前。

### 6.5 落地方案

- **第一步（地基）**：循环骨架 + 轮数上限。先让「收敛」达标。
- **第二步**：工具回填严格化——`toolCallId` 一一对应，串行执行、按序回填。让「顺序正确」达标。
- **第三步**：把超时/防循环抽成 `LoopHooks`，加单步超时。让「策略可替换」达标。
- **第四步（进阶）**：加重复检测、并行执行（只对无依赖工具）。

关键顺序：**先保证「不会死、顺序对」，再谈「跑得快、更聪明」。** 收敛和顺序是正确性，并行和防重复是优化。

### 6.6 反模式（避坑）

1. **❌ 回填时不带 `toolCallId`，只靠位置对齐**——结果：并行执行或顺序微调后，模型张冠李戴。正确做法：每个 tool 结果都带 id。
2. **❌ 把超时/重试逻辑硬编码在 while 循环里**——结果：换个场景改核心。正确做法：钩子化。
3. **❌ 只做「轮数上限」就认为防循环够了**——结果：模型可能在轮数内用不同参数反复空转（语义死循环），轮数上限救不了。正确做法：轮数上限 + 超时 + 重复检测三层。
4. **❌ 并行执行时，把有依赖的工具也并发**——结果：后一个工具拿到过时数据。正确做法：只对声明为「无依赖」的工具并行。

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
