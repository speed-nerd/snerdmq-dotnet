<div align="center">
  <img src="https://raw.githubusercontent.com/speed-nerd/snerdmq/main/assets/snerdmq-transparent.png" width="200" alt="SnerdMQ Logo"/>
  <h1>SnerdMQ .NET SDK (v0.4.1)</h1>

  [![Docs](https://img.shields.io/badge/docs-speed--nerd.github.io-blue)](https://speed-nerd.github.io/docs/sdks/dotnet/)
</div>


## Features
- **Zero Configuration**: No connection strings, no ports, no firewall rules.
- **Native Task Parallelism**: Leverages C#'s massive `async`/`Task` ThreadPool.
- **ASP.NET Core Friendly**: Never blocks the main event loop.
- **Bulletproof Durability**: Uses OS-level file locking for ACID compliance.

> 📚 **Full Documentation & Advanced Features:** Check out the [official C# / .NET SDK documentation](https://speed-nerd.github.io/docs/sdks/dotnet/) on our docs site!

## ✨ v0.4.1 AI Features
- **Worker Pools**: Prevent slow generative AI tasks from starving fast DB tasks by dedicating threads to specific pools (e.g. `"urgent"`).
- **Sharded Queues**: Distribute load across multiple queue nodes safely using file-backed lock sharding (`MaxLocalShards`).
- **Smart API Rate-Limiting**: Natively tracks `rateLimitGroup` execution velocity to prevent 429 "Too Many Requests" API errors.
- **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
- **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
- **Job Chaining (DAGs)**: Define complex workflow dependencies natively. Tasks wait in a blocked state until their parent tasks succeed.
- **Progress Streaming & Live Dashboard**: Handlers can stream progress updates to a built-in React UI dashboard served by the SDK.

### ⚙️ Advanced Task Configuration (v0.4.1)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters:

* **`autoDedupe` (`bool`)**: If set to `true`, the daemon computes a cryptographic hash of the `taskType` and `data`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgencyScore` (`double`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rateLimitGroup` (`string`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`maxPerMinute` (`int`)**: Used in conjunction with `rateLimitGroup`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`executeAt` (`DateTime?`)**: A timestamp of when the job should be executed in the future.
* **`retryAfterHours` (`double`)**: Backoff in **hours** before a failed job is retried (default `0.0`). See *Cron Jobs vs. Retryable Jobs* below.
* **`cron` (`string`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.
* **`webhookUrl` (`string`)**: By providing a webhook URL, SnerdMQ will completely bypass your local .NET handlers and dispatch the task payload via an HTTP POST request directly to the specified URL.
* **`maxExecutionSeconds` (`int?`)**: Optional hard timeout in seconds. If execution takes longer, it's marked as failed.
* **`triggerAfterIds` (`List<string>`)**: A list of parent task IDs that must complete successfully before this task is allowed to dispatch. Enables complex DAG workflows natively within the queue.
* **`pool` (`string`)**: Dedicate this task to a specific worker pool (e.g. `"urgent"`). Initialize pool sizes via `MaxWorkers` in the constructor.

### Note on Hard Timeouts (`maxExecutionSeconds`)
When `maxExecutionSeconds` is provided, the .NET SDK wraps the execution of your handler using `Task.WhenAny` with `Task.Delay`. If the task takes longer than the timeout, the SDK will mark it as failed and abandon the handler. The background Rust daemon also enforces this timeout at the IPC level.

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhookUrl`, the internal background processor will skip any registered handlers (`queue.RegisterHandler`) and directly invoke the HTTP endpoint.

If the HTTP endpoint returns a non-200 status code, it triggers a retry. If it permanently fails (reaches `maxRetries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhookUrl` but with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the new scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retryAfterHours` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retryAfterHours` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

## Installation
*(Coming soon to NuGet)*
```bash
dotnet add package SnerdMQ
```

## Quick Start
```csharp
using SnerdMQ;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        // 1. Initialize the Queue Orchestrator
        using var queue = new SnerdQueue();

        // 2. Register async job handlers
        queue.RegisterHandler("send_email", async (jsonData) =>
        {
            Console.WriteLine($"Sending email with data: {jsonData}");
            await Task.Delay(1000); // Simulate network request
        });

        // 3. Start listening for jobs in the background ThreadPool
        queue.StartListening();

        // 4. Enqueue a persistent background job!
        await queue.Enqueue(
            taskId: "email_123",
            taskType: "send_email",
            jsonData: "{\"user\":\"john.wick@example.com\"}",
            maxRetries: 3,
            retryAfterHours: 0.5,      // Wait 30 minutes before retrying a failed job
            rateLimitGroup: "sendgrid_api",
            maxPerMinute: 100
        );

        // 5. Need scheduling, deduplication, or serverless execution? All
        // orchestration options are opt-in — combine only what you need:
        await queue.Enqueue(
            taskId: "email_digest_1",
            taskType: "send_email",
            jsonData: "{\"user\":\"john.wick@example.com\",\"subject\":\"Daily Digest\"}",
            maxRetries: 3,
            retryAfterHours: 0.0,
            rateLimitGroup: null,      // No rate limit group
            maxPerMinute: null,        // No max-per-minute cap
            autoDedupe: true,          // Drop identical pending payloads
            urgencyScore: 0.99,        // Float to the front of the queue
            executeAt: null,
            cron: "0 8 * * *",         // Run every day at 08:00
            webhookUrl: "https://api.example.com/webhook", // Execute via HTTP instead of local handlers
            maxExecutionSeconds: 300,  // Hard timeout
            triggerAfterIds: new List<string> { "parent-123" }, // Wait for parent tasks
            pool: "urgent"             // Dedicate to a specific worker pool
        );

        // Prevent console app from exiting
        await Task.Delay(-1);
    }
}
```

## How it works
This SDK spawns a highly-optimized Rust binary as a child process and communicates with it asynchronously over standard I/O pipes. The Rust engine handles all the complex file-locking, retries, and persistence, while invoking your C# delegates natively!

### ☠️ Dead Letter Queue (Handling Permanent Failures)

When a task fails repeatedly and exhausts its `maxRetries`, the SnerdMQ daemon permanently moves it to the Dead Letter Queue. You can hook into this event to alert your team, update your database, or send a Slack message by registering a Max Retry Handler.

```csharp
// 5. Catch tasks that have permanently failed (Dead Letter Queue)
queue.RegisterMaxRetryHandler("send_email", (data) => {
    Console.WriteLine($"Email task failed after all retries! Data: {data}");
});
```

---

## 📊 Live Dashboard

SnerdMQ ships with a built-in **React UI dashboard** served directly by the SDK over its embedded `HttpListener` — no extra services or ports to manage in your infrastructure. It gives you a real-time window into your queue:

- **Live stats**: total enqueued, processed, and failed jobs
- **Recent Jobs table**: per-task status (`queued`, `active`, `completed`, `failed`, `dead_letter`), retry counts, and badges showing which features a task uses (cron / webhook / timeout)
- **Real-time Progress Stream**: live output from `YieldProgress` calls in your handlers

```csharp
using var queue = new SnerdQueue();

// Start the built-in dashboard on http://localhost:9090
queue.StartDashboard(9090);

// ... register handlers, start listening, enqueue jobs ...
```

Then open **http://localhost:9090** in your browser. Updates are pushed to the page over WebSocket the moment jobs change state (with an automatic HTTP polling fallback), and the dashboard also exposes a small JSON API (`/api/stats`, `/api/tasks`, `/api/progress`) if you want to build your own tooling on top.

> **Note:** The dashboard serves its `static/index.html` from a `static/` folder next to your application's base directory, so make sure the dashboard bundle ships with your deployment. `StartDashboard` only serves the UI — your jobs keep running whether or not the dashboard is open.

---

## 📡 Progress Reporting

Long-running handlers can stream live updates to the Dashboard's Progress Stream (ideal for streaming LLM tokens or multi-step ETL work):

```csharp
queue.RegisterHandler("generate_report", async (jsonData) =>
{
    for (int step = 1; step <= 10; step++)
    {
        await DoWorkAsync(step);
        queue.YieldProgress($"Step {step}/10 complete");
    }
});
```

> `YieldProgress` must be called **inside a task handler** — the SDK tracks which task is currently executing so each update lands on the right job in the dashboard.

---

## 🧩 Queue Topology: One Queue or Many?

### ✅ Recommended: one queue, all job types (singleton)

Each `SnerdQueue` client spawns its own Rust daemon and **exclusively owns** its storage directory (`.snerdata` by default). The recommended pattern is **one client per application process**: register every job type on it and serve a single shared dashboard:

```csharp
using SnerdMQ;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        // ONE queue client for the whole app
        using var queue = new SnerdQueue();

        // Job type #1: image processing
        queue.RegisterHandler("process_image", (jsonData) =>
        {
            Console.WriteLine($"Processing image: {jsonData}");
        });

        // Job type #2: OTP emails — same queue, same daemon
        queue.RegisterHandler("send_otp_email", (jsonData) =>
        {
            Console.WriteLine($"Sending OTP: {jsonData}");
        });

        queue.StartListening();

        // Both job types flow through the exact same queue
        await queue.Enqueue("img-1", "process_image", "{\"image_id\":\"abc123\"}", 3, 0.5);
        await queue.Enqueue("otp-1", "send_otp_email", "{\"to\":\"john@wick.com\"}", 3, 0.5);

        // ONE dashboard shows every job type
        queue.StartDashboard(8080);
    }
}
```

All job types share everything: the same persistent job log, retry/DLQ pipeline, rate-limit state, stats — and one dashboard at `http://localhost:8080` showing all of them.

### 🚫 Same storage twice = fails fast

The daemon takes an **exclusive OS-level lock** on its storage directory at startup. A second client on the same storage fails instead of silently double-executing your jobs:

```csharp
using var first = new SnerdQueue();  // ✅ owns .snerdata
using var second = new SnerdQueue(); // ❌ daemon refuses to start:
// "Another daemon is already running on storage '.snerdata'"
```

This applies across processes too — in a multi-worker deployment, each worker must either use its own storage directory or talk to a single shared daemon. To safely scale on the same disk without double-executing jobs, initialize with `MaxLocalShards`:
```csharp
using var queue = new SnerdQueue(maxLocalShards: 4, maxWorkers: new Dictionary<string, int> { ["urgent"] = 5 });
```

### 🔀 Need multiple queues? Give each one its own storage

```csharp
using var images = new SnerdQueue(null, ".snerdata-images");
using var emails = new SnerdQueue(null, ".snerdata-emails");

images.StartDashboard(8080); // separate dashboards, so separate ports
emails.StartDashboard(8081);
```

Now you have two fully independent engines: separate job logs, separate rate-limit state, separate dashboards. Only split when you actually need isolation (different teams, different retention, independent monitoring) — otherwise the singleton is simpler and recommended.

---

## 🌍 Advanced: Distributed Scaling

Because the daemon exclusively locks its storage directory, scaling horizontally means **one queue per server**, each with its own storage. Your load balancer routes requests across servers, and every server processes the jobs it enqueued:

```csharp
// Each server runs its own daemon on its own storage dir (local disk works fine)
using var queue = new SnerdQueue(null, "/var/data/snerd"); // per-server storage
```

A shared network drive (AWS EFS or NFS) is still a good home for that storage when a single instance needs durable state — e.g. a container that restarts but must keep its queue. Native OS file locking (`flock`) keeps writes safe — no Redis required.


---

## 🚀 Advanced Orchestration

### 🏊 Worker Pools

SnerdMQ supports dedicating worker resources to specific tasks so that slow AI generation tasks don't starve fast database updates.

In the SDK, simply assign a pool name when enqueueing the task using the `pool` parameter. When running the daemon, you can allocate concurrent workers per pool using the environment variable `SNERD_POOLS="default:100,urgent:50"`.

### 🔗 Job Chaining (DAGs)

You can define complex workflow dependencies natively. Tasks will wait in a blocked state until their parent tasks successfully complete.

Simply pass an array of parent task IDs to the `trigger_after_ids` parameter when enqueueing. This easily unlocks Fan-In and Linear workflows natively within the queue.

### 🍕 Sharded Queues (Scaling Out)

SnerdMQ natively supports distributed execution across multiple servers while acting as a single logical queue. Just mount a shared storage drive (like AWS EFS) and boot multiple daemons. They will automatically lock and negotiate ownership of shards. No config required in the SDK for enqueueing! Just tell the daemon how many shards to claim on boot:

```csharp
// Boot a multi-tenant daemon that owns up to 4 shards locally
using var queue = new SnerdQueue(maxLocalShards: 4);
```

```csharp
// 1. Worker Pools: Route tasks to the 'urgent' pool
queue.Enqueue(
    taskId: "payment-job", 
    taskType: "process_payment", 
    data: new { amount = 100 },
    pool: "urgent"
);

// 2. Job Chaining: Block execution until parents succeed
queue.Enqueue(
    taskId: "final-job", 
    taskType: "send_report", 
    data: new { id = 1 },
    triggerAfterIds: new List<string> { "parent-job-1", "parent-job-2" }
);
```


### 🕒 Cron & Scheduled Jobs
```csharp
// Run every day at 08:00
queue.Enqueue(
    taskId: "daily-digest", 
    taskType: "send_email", 
    data: new { template = "daily" },
    cron: "0 8 * * *"
);
```

### 🛑 Hard Timeouts
```csharp
// Forcefully kill if running > 5 mins
queue.Enqueue(
    taskId: "risky-task", 
    taskType: "process_data", 
    data: new { },
    maxExecutionSeconds: 300
);
```

### 🌐 Webhook Callbacks
```csharp
// Execute via HTTP instead of local handlers
queue.Enqueue(
    taskId: "serverless-task", 
    taskType: "resize_image", 
    data: new { img = "cat.jpg" },
    webhookUrl: "https://api.example.com/webhooks/snerdmq"
);
```

*Built with ❤️ for John Wick tier engineering.*


## Architecture Best Practices

When building production applications with SnerdMQ, it is recommended to initialize the queue as a Singleton, isolate your domain workers into separate files/functions, use Dead Letter Queues (DLQ) for failed tasks via `RegisterMaxRetryHandler`, and ensure manual graceful shutdown. The embedded Dashboard UI can also be easily served from the same instance.

```csharp
using System;
using System.Threading.Tasks;
using SnerdMQ;

class Program
{
    static async Task Main(string[] args)
    {
        var queue = new SnerdQueue(storagePath: "./.snerdata");

        // Email Workers
        queue.RegisterHandler("send_email", async (data) =>
        {
            var email = (string)data["email"];
            Console.WriteLine($"Sending email to {email}...");
        });

        queue.RegisterMaxRetryHandler("send_email", async (data) =>
        {
            var email = (string)data["email"];
            Console.WriteLine($"Email to {email} failed permanently. Dead letter processing...");
        });

        // Image Workers
        queue.RegisterHandler("process_image", async (data) =>
        {
            Console.WriteLine($"Processing image {(string)data["imageId"]}...");
        });

        queue.StartDashboard(8080);

        // Graceful shutdown
        Console.CancelKeyPress += (s, e) =>
        {
            e.Cancel = true;
            queue.Shutdown();
        };

        await queue.StartListening();
    }
}
```
