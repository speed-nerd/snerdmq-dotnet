using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace SnerdMQ
{
    public class SnerdQueue : IDisposable
    {
        private readonly string _binaryPath;
        private readonly string _storagePath;
        private readonly ConcurrentDictionary<string, Func<string, Task>> _handlers = new();
        private readonly ConcurrentDictionary<string, Func<string, Task>> _maxRetryHandlers = new();
        
        private Process _process;
        private StreamWriter _writer;
        private CancellationTokenSource _cts = new();
        private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _pendingEnqueues = new();
        private static readonly AsyncLocal<string> CurrentTaskId = new AsyncLocal<string>();
        private readonly ConcurrentDictionary<System.Net.WebSockets.WebSocket, byte> _wsClients = new ConcurrentDictionary<System.Net.WebSockets.WebSocket, byte>();


        private readonly int? _maxLocalShards;
        private readonly int? _maxWorkers;

        public SnerdQueue(string binaryPath = null, string storagePath = null, int? maxLocalShards = null, int? maxWorkers = null)
        {
            _binaryPath = binaryPath ?? SnerdmqInstaller.EnsureDownloadedAsync().GetAwaiter().GetResult();
            _storagePath = storagePath;
            _maxLocalShards = maxLocalShards;
            _maxWorkers = maxWorkers;

            if (string.IsNullOrEmpty(_binaryPath))
            {
                throw new InvalidOperationException("[Snerd] Binary path cannot be null. Installer failed or path not provided.");
            }
        }

        public void RegisterHandler(string taskType, Func<string, Task> callback)
        {
            _handlers[taskType] = callback;
            if (_process != null && !_process.HasExited)
            {
                SendMessage($"{{\"action\":\"register\",\"task_type\":\"{taskType}\"}}");
            }
        }

        public void RegisterHandler(string taskType, Action<string> callback)
        {
            _handlers[taskType] = (data) =>
            {
                callback(data);
                return Task.CompletedTask;
            };
        }

        public void RegisterMaxRetryHandler(string taskType, Func<string, Task> callback)
        {
            _maxRetryHandlers[taskType] = callback;
        }

        public void RegisterMaxRetryHandler(string taskType, Action<string> callback)
        {
            _maxRetryHandlers[taskType] = (data) =>
            {
                callback(data);
                return Task.CompletedTask;
            };
        }

        public void StartListening()
        {
            var psi = new ProcessStartInfo
            {
                FileName = _binaryPath,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            if (!string.IsNullOrEmpty(_storagePath))
            {
                psi.Arguments = $"\"{_storagePath}\"";
            }

            if (_maxLocalShards.HasValue)
            {
                psi.EnvironmentVariables["SNERD_MAX_SHARDS"] = _maxLocalShards.Value.ToString();
            }
            if (_maxWorkers.HasValue)
            {
                psi.EnvironmentVariables["SNERD_MAX_WORKERS"] = _maxWorkers.Value.ToString();
            }

            _process = new Process { StartInfo = psi };
            _process.Start();

            _writer = _process.StandardInput;

            // Start reading stdout asynchronously in a fire-and-forget Task
            _ = Task.Run(ReadStdoutAsync, _cts.Token);

            foreach (var taskType in _handlers.Keys)
            {
                SendMessage($"{{\"action\":\"register\",\"task_type\":\"{taskType}\"}}");
            }
        }

        public Task Enqueue(string taskId, string taskType, string jsonData, int maxRetries, double retryAfterHours)
        {
            return Enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, null, null, null, null, null, null, null);
        }

        public Task Enqueue(string taskId, string taskType, string jsonData, int maxRetries, double retryAfterHours, string rateLimitGroup, int? maxPerMinute)
        {
            return Enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, null, null, null, null, null);
        }

        public Task Enqueue(string taskId, string taskType, string jsonData, int maxRetries, double retryAfterHours, string rateLimitGroup, int? maxPerMinute, bool? autoDedupe)
        {
            return Enqueue(taskId, taskType, jsonData, maxRetries, retryAfterHours, rateLimitGroup, maxPerMinute, autoDedupe, null, null, null, null);
        }

        public Task Enqueue(string taskId, string taskType, string jsonData, int maxRetries, double retryAfterHours, string rateLimitGroup, int? maxPerMinute, bool? autoDedupe, double? urgencyScore, DateTime? executeAt = null, string cron = null, string webhookUrl = null, int? maxExecutionSeconds = null, System.Collections.Generic.List<string> triggerAfterIds = null, string pool = null)
        {
            if (_process == null || _process.HasExited)
            {
                return Task.FromException(new InvalidOperationException("[Snerd] Cannot enqueue task: Queue is not running."));
            }

            var tcs = new TaskCompletionSource<bool>();
            _pendingEnqueues[taskId] = tcs;

            string escapedJson = jsonData.Replace("\"", "\\\"");
            
            var sb = new System.Text.StringBuilder();
            sb.Append($"{{\"action\":\"enqueue\",\"task_id\":\"{taskId}\",\"task_type\":\"{taskType}\",\"task_data\":\"{escapedJson}\",\"max_retries\":{maxRetries},\"retry_after_hours\":{retryAfterHours.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
            
            if (rateLimitGroup != null)
            {
                sb.Append($",\"rate_limit_group\":\"{rateLimitGroup}\"");
            }
            if (maxPerMinute.HasValue)
            {
                sb.Append($",\"max_per_minute\":{maxPerMinute.Value}");
            }
            if (autoDedupe.HasValue)
            {
                sb.Append($",\"auto_dedupe\":{(autoDedupe.Value ? "true" : "false")}");
            }
            if (urgencyScore.HasValue)
            {
                sb.Append($",\"urgency_score\":{urgencyScore.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)}");
            }
            if (executeAt.HasValue)
            {
                // Format as ISO 8601 string
                sb.Append($",\"execute_at\":\"{executeAt.Value.ToString("O")}\"");
            }
            if (!string.IsNullOrEmpty(cron))
            {
                sb.Append($",\"cron\":\"{cron}\"");
            }
            if (!string.IsNullOrEmpty(webhookUrl))
            {
                sb.Append($",\"webhook_url\":\"{webhookUrl}\"");
            }
            if (maxExecutionSeconds.HasValue)
            {
                sb.Append($",\"max_execution_seconds\":{maxExecutionSeconds.Value}");
            }
            if (triggerAfterIds != null && triggerAfterIds.Count > 0)
            {
                sb.Append(",\"trigger_after_ids\":[");
                sb.Append(string.Join(",", triggerAfterIds.ConvertAll(id => $"\"{id}\"")));
                sb.Append("]");
            }
            if (!string.IsNullOrEmpty(pool))
            {
                sb.Append($",\"pool\":\"{pool}\"");
            }
            sb.Append("}");
            
            SendMessage(sb.ToString());
            return tcs.Task;
        }

        private void SendMessage(string json)
        {
            if (_writer == null) return;
            if (_process.HasExited) return;
            lock (_writer)
            {
                try
                {
                    _writer.WriteLine(json);
                    _writer.Flush();
                }
                catch (IOException) { } // Daemon died, pipe broken
                catch (ObjectDisposedException) { } // Stream already closed
            }
        }

        private async Task ReadStdoutAsync()
        {
            try
            {
                while (!_cts.Token.IsCancellationRequested && !_process.StandardOutput.EndOfStream)
                {
                    string line = await _process.StandardOutput.ReadLineAsync();
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        ProcessLine(line.Trim());
                    }
                }
            }
            catch (Exception ex)
            {
                if (!_cts.Token.IsCancellationRequested)
                    Console.Error.WriteLine($"[Snerd] Background Reader Exception: {ex.Message}");
            }
            finally
            {
                // Engine is gone — it can never ack. Fault all pending enqueues
                // so callers fail fast instead of awaiting forever.
                foreach (var kvp in _pendingEnqueues)
                {
                    if (_pendingEnqueues.TryRemove(kvp.Key, out var tcs))
                    {
                        tcs.TrySetException(new InvalidOperationException(
                            $"[Snerd] Engine terminated before ack for task '{kvp.Key}'"));
                    }
                }
            }
        }

        private void ProcessLine(string line)
        {
            // Lightweight RegEx parsing to avoid external dependencies like Newtonsoft.Json
            string action = ExtractJsonField(line, "action");
            if (action == null) return;

            if (action == "execute")
            {
                string taskId = ExtractJsonField(line, "task_id");
                string taskType = ExtractJsonField(line, "task_type");
                string taskData = ExtractJsonField(line, "task_data");
                string maxExecutionStr = ExtractJsonNumberField(line, "max_execution_seconds");
                int? maxExecutionSeconds = maxExecutionStr != null ? int.Parse(maxExecutionStr) : null;

                if (taskId == null || taskType == null) return;

                if (!_handlers.TryGetValue(taskType, out var handler))
                {
                    SendMessage($"{{\"action\":\"result\",\"task_id\":\"{taskId}\",\"status\":\"error\",\"error_msg\":\"No handler registered\"}}");
                    return;
                }

                // Fire-and-forget the user's workload onto the ThreadPool so we don't block stdout
                _ = Task.Run(async () =>
                {
                    try
                    {
                        CurrentTaskId.Value = taskId;
                        string unescapedData = taskData != null ? taskData.Replace("\\\"", "\"").Replace("\\\\", "\\") : "";
                        var handlerTask = handler(unescapedData);
                        
                        if (maxExecutionSeconds.HasValue)
                        {
                            var timeoutTask = Task.Delay(maxExecutionSeconds.Value * 1000);
                            var completedTask = await Task.WhenAny(handlerTask, timeoutTask);
                            if (completedTask == timeoutTask)
                            {
                                SendMessage($"{{\"action\":\"result\",\"task_id\":\"{taskId}\",\"status\":\"error\",\"error_msg\":\"Task execution timed out after {maxExecutionSeconds.Value} seconds\"}}");
                                return;
                            }
                        }
                        
                        await handlerTask;
                        SendMessage($"{{\"action\":\"result\",\"task_id\":\"{taskId}\",\"status\":\"success\"}}");
                    }
                    catch (Exception ex)
                    {
                        string errorMsg = ex.Message.Replace("\"", "'");
                        SendMessage($"{{\"action\":\"result\",\"task_id\":\"{taskId}\",\"status\":\"error\",\"error_msg\":\"{errorMsg}\"}}");
                    }
                });
            }
            else if (action == "ack")
            {
                string taskId = ExtractJsonField(line, "task_id");
                if (taskId != null && _pendingEnqueues.TryRemove(taskId, out var tcs))
                {
                    tcs.TrySetResult(true);
                }
            }
            else if (action == "error")
            {
                string taskId = ExtractJsonField(line, "task_id");
                string message = ExtractJsonField(line, "message");
                if (taskId != null && _pendingEnqueues.TryRemove(taskId, out var tcs))
                {
                    tcs.TrySetException(new InvalidOperationException(message));
                }
                else
                {
                    Console.Error.WriteLine($"[Snerd] Error from engine: {message}");
                }
            }
            else if (action == "progress")
            {
                var buffer = System.Text.Encoding.UTF8.GetBytes(line);
                foreach (var ws in _wsClients.Keys)
                {
                    if (ws.State == System.Net.WebSockets.WebSocketState.Open)
                    {
                        _ = ws.SendAsync(new ArraySegment<byte>(buffer), System.Net.WebSockets.WebSocketMessageType.Text, true, CancellationToken.None);
                    }
                }
            }
            else if (action == "max_retries_reached")
            {
                string taskId = ExtractJsonField(line, "task_id");
                string taskType = ExtractJsonField(line, "task_type");

                if (taskId != null && taskType != null && _maxRetryHandlers.TryGetValue(taskType, out var handler))
                {
                    string taskData = ExtractJsonField(line, "task_data");
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            CurrentTaskId.Value = taskId;
                            string unescapedData = taskData != null ? taskData.Replace("\\\"", "\"").Replace("\\\\", "\\") : "";
                            await handler(unescapedData);
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine($"[Snerd] Error in max retry handler for task {taskId}: {ex.Message}");
                        }
                    });
                }
                else
                {
                    Console.WriteLine($"[Snerd] Dead Letter Queue: Task {taskId} ({taskType}) permanently failed.");
                }
            }
        }

        private string ExtractJsonField(string json, string key)
        {
            var match = Regex.Match(json, $"\"{key}\"\\s*:\\s*\"(.*?)(?<!\\\\)\"");
            return match.Success ? match.Groups[1].Value : null;
        }

        private string ExtractJsonNumberField(string json, string key)
        {
            var match = Regex.Match(json, $"\"{key}\"\\s*:\\s*([0-9.]+)");
            return match.Success ? match.Groups[1].Value : null;
        }

        public void Dispose()
        {
            _cts.Cancel();
            if (_process != null && !_process.HasExited)
            {
                _process.Kill();
                _process.Dispose();
            }
        }
    
        public void YieldProgress(string data)
        {
            var taskId = CurrentTaskId.Value;
            if (taskId == null)
            {
                throw new InvalidOperationException("[Snerd] YieldProgress must be called within a task handler context.");
            }
            string escapedData = data != null ? data.Replace("\"", "\\\"") : "";
            SendMessage($"{{\"action\":\"progress\",\"task_id\":\"{taskId}\",\"data\":\"{escapedData}\"}}");
        }

        public void StartDashboard(int port = 8080)
        {
            var listener = new System.Net.HttpListener();
            listener.Prefixes.Add($"http://*:{port}/");
            listener.Start();
            Console.WriteLine($"[Snerd] Dashboard running on http://localhost:{port}");

            _ = Task.Run(async () =>
            {
                while (listener.IsListening)
                {
                    try
                    {
                        var context = await listener.GetContextAsync();
                        
                        context.Response.AppendHeader("Access-Control-Allow-Origin", "*");
                        context.Response.AppendHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
                        
                        if (context.Request.HttpMethod == "OPTIONS")
                        {
                            context.Response.StatusCode = 204;
                            context.Response.Close();
                            continue;
                        }

                        if (context.Request.IsWebSocketRequest)
                        {
                            var wsContext = await context.AcceptWebSocketAsync(null);
                            var ws = wsContext.WebSocket;
                            _wsClients.TryAdd(ws, 0);
                            
                            _ = Task.Run(async () =>
                            {
                                var buffer = new byte[1024];
                                try
                                {
                                    while (ws.State == System.Net.WebSockets.WebSocketState.Open)
                                    {
                                        await ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                                    }
                                }
                                catch { }
                                finally
                                {
                                    _wsClients.TryRemove(ws, out _);
                                }
                            });
                            continue;
                        }

                        if (context.Request.HttpMethod == "GET")
                        {
                            if (context.Request.Url.AbsolutePath == "/")
                            {
                                string htmlPath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "..", "static", "index.html");
                                if (!System.IO.File.Exists(htmlPath))
                                {
                                    htmlPath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "static", "index.html");
                                }
                                if (System.IO.File.Exists(htmlPath))
                                {
                                    context.Response.ContentType = "text/html";
                                    byte[] buf = System.IO.File.ReadAllBytes(htmlPath);
                                    context.Response.ContentLength64 = buf.Length;
                                    await context.Response.OutputStream.WriteAsync(buf, 0, buf.Length);
                                }
                                else
                                {
                                    context.Response.StatusCode = 404;
                                }
                            }
                            else if (context.Request.Url.AbsolutePath == "/api/stats")
                            {
                                int enqueued = 0, processed = 0, failed = 0;
                                var tasksMap = new System.Collections.Generic.Dictionary<string, string>();
                                string storage = string.IsNullOrEmpty(_storagePath) ? "./.snerdata" : _storagePath;
                                string tasksPath = System.IO.Path.Combine(storage, "tasks", "tasks.log");
                                if (System.IO.File.Exists(tasksPath))
                                {
                                    foreach (var line in System.IO.File.ReadLines(tasksPath))
                                    {
                                        if (string.IsNullOrWhiteSpace(line)) continue;
                                        string tId = ExtractJsonField(line, "taskId");
                                        if (tId != null) tasksMap[tId] = line;
                                    }
                                }
                                foreach (var t in tasksMap.Values)
                                {
                                    enqueued++;
                                    if (t.Contains("\"deletedAt\":"))
                                    {
                                        if (t.Contains("\"LastJobError\":")) failed++;
                                        else processed++;
                                    }
                                }
                                string res = $"{{\"enqueued\":{enqueued},\"processed\":{processed},\"failed\":{failed}}}";
                                context.Response.ContentType = "application/json";
                                byte[] buf = System.Text.Encoding.UTF8.GetBytes(res);
                                context.Response.ContentLength64 = buf.Length;
                                await context.Response.OutputStream.WriteAsync(buf, 0, buf.Length);
                            }
                            else if (context.Request.Url.AbsolutePath == "/api/tasks")
                            {
                                var tasksMap = new System.Collections.Generic.Dictionary<string, string>();
                                string storage = string.IsNullOrEmpty(_storagePath) ? "./.snerdata" : _storagePath;
                                string tasksPath = System.IO.Path.Combine(storage, "tasks", "tasks.log");
                                if (System.IO.File.Exists(tasksPath))
                                {
                                    foreach (var line in System.IO.File.ReadLines(tasksPath))
                                    {
                                        if (string.IsNullOrWhiteSpace(line)) continue;
                                        string tId = ExtractJsonField(line, "taskId");
                                        if (tId != null) tasksMap[tId] = line;
                                    }
                                }
                                
                                var sb = new System.Text.StringBuilder("[");
                                bool first = true;
                                foreach (var t in tasksMap.Values)
                                {
                                    string tId = ExtractJsonField(t, "taskId");
                                    string tType = ExtractJsonField(t, "taskType");
                                    string rCount = ExtractJsonNumberField(t, "retryCount") ?? "0";
                                    string mRetries = ExtractJsonNumberField(t, "maxRetries") ?? "3";
                                    string status;
                                    if (t.Contains("\"deletedAt\":")) {
                                        bool hasError = t.Contains("\"LastJobError\":");
                                        if (hasError && int.TryParse(rCount, out int rc) && int.TryParse(mRetries, out int mr) && rc >= mr) {
                                            status = "dead_letter";
                                        } else if (hasError) {
                                            status = "failed";
                                        } else {
                                            status = "completed";
                                        }
                                    } else if (t.Contains("\"LastJobError\":")) {
                                        status = "failed";
                                    } else {
                                        status = "queued";
                                        string execAt = ExtractJsonField(t, "executeAt");
                                        if (execAt != null && DateTimeOffset.TryParse(execAt, out var execTime) && execTime <= DateTimeOffset.UtcNow) {
                                            status = "active";
                                        }
                                    }
                                    
                                    string rAfter = ExtractJsonField(t, "retryAfterTime");
                                    string cronExpr = ExtractJsonField(t, "cronExpression");
                                    string webhookUrl = ExtractJsonField(t, "webhookUrl");
                                    string maxExecSecs = ExtractJsonNumberField(t, "maxExecutionSeconds");
                                    
                                    if (!first) sb.Append(",");
                                    sb.Append($"{{\"id\":\"{tId}\",\"type\":\"{tType}\",\"status\":\"{status}\",\"progress\":0");
                                    if (rCount != null) sb.Append($",\"retryCount\":{rCount}");
                                    if (mRetries != null) sb.Append($",\"maxRetries\":{mRetries}");
                                    if (rAfter != null) sb.Append($",\"retryAfterTime\":\"{rAfter}\"");
                                    if (cronExpr != null) sb.Append($",\"cronExpression\":\"{cronExpr}\"");
                                    if (webhookUrl != null) sb.Append($",\"webhookUrl\":\"{webhookUrl}\"");
                                    if (maxExecSecs != null) sb.Append($",\"maxExecutionSeconds\":{maxExecSecs}");
                                    sb.Append("}");
                                    first = false;
                                }
                                sb.Append("]");
                                context.Response.ContentType = "application/json";
                                byte[] buf = System.Text.Encoding.UTF8.GetBytes(sb.ToString());
                                context.Response.ContentLength64 = buf.Length;
                                await context.Response.OutputStream.WriteAsync(buf, 0, buf.Length);
                            }
                            else
                            {
                                context.Response.StatusCode = 404;
                            }
                        }
                        
                        context.Response.Close();
                    }
                    catch { }
                }
            });
        }

    }
}
