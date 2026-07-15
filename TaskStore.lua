--[[
=================================================================================
    TASKSTORE: The Upgraded pcall Framework
=================================================================================
    [ WHERE DOES THIS GO? ]
    This is a Module Script. just incase
    For Best Practice, put it in:
        -> ReplicatedStorage (if BOTH client and server scripts need it)
        -> ServerScriptService (if only Server scripts/DataStores need it)

    [ WHY IS THIS BETTER THAN RAW PCALL? ]
    1. ZERO NESTING: Stop writing messy "if not success then" blocks everywhere.
    2. SMART RETRIES: Automatically handles network blips with exponential backoff 
       (waits longer after each failure so you don't spam servers).
    3. STATE MACHINE: Tracks whether a task is "Pending", "Resolved", or "Rejected".
    4. GRACEFUL FALLBACKS: Built-in `.Catch()` lets you load backup data if the main
       action completely dies.
    5. CHAINING: `.Then()`, `.Catch()`, and `.Finally()` all return self for chaining
       without nesting.
    6. TIMEOUTS: Built-in timeout support prevents hanging functions from blocking forever.
    7. CANCELLATION: `:Cancel()` stops a running task. `TaskStore.Any()` auto-cancels
       losing tasks after the first success.
    8. PARALLEL TASKS: `TaskStore.Any()` and `TaskStore.All()` run multiple tasks
       concurrently — like Promise.race() and Promise.all().

    [ HOW IT WORKS — DEV NOTES ]
    -----------------------------------------------------------------------------
    1. TaskStore.new(fn) creates a Task object with Status = "Pending".
       The action function is stored but NOT executed yet.

    2. :Await(maxRetries, delayBetween, timeout) executes the action inside a pcall.
       - If timeout is provided, each attempt runs in a separate thread (task.spawn)
         and is polled every 0.1s. If it exceeds the timeout, the thread is cancelled
         (task.cancel) and the attempt counts as a failure.
       - Between failed attempts, exponential backoff is applied:
         delayBetween * 2^(attempt-1) -> 1s, 2s, 4s, 8s...
       - After all retries, Status becomes "Resolved" (success) or "Rejected" (failure).
       - If .Then(), .Catch(), or .Finally() callbacks were registered BEFORE Await,
         they fire at the end of Await.

    3. :Then(cb) registers a success callback. Multiple calls are supported — each
       adds to the list and all fire in order. If the task is already Resolved,
       the callback fires immediately. Returns self for chaining.

    4. :Catch(cb) runs ONLY if Status == "Rejected". Can be called BEFORE or AFTER
       :Await(). Returns self for chaining (not data — use side effects in the callback).

    5. :Finally(cb) runs regardless of outcome. If Status is already Resolved/Rejected,
       fires immediately. Returns self for chaining.

    6. :Cancel() marks a task as cancelled. If :Await() is currently running, it stops
       retrying and sets Status to "Rejected" with Error "Task was cancelled".

    7. TaskStore.Any(tasks, ...) runs all tasks concurrently via task.spawn.
       Returns the first task that succeeds. All remaining tasks are automatically
       cancelled via :Cancel(). If all fail, returns nil.

    8. TaskStore.All(tasks, ...) runs all tasks concurrently via task.spawn.
       Returns an array of {success, result, task} for each task.
=================================================================================
]]

local TaskStore = {}
TaskStore.__index = TaskStore

-- Defines the exact status of our task at any given second
export type TaskStatus = "Pending" | "Resolved" | "Rejected"

--[[
    TaskStore.new(actionFunction)
    -----------------------------------------------------------------------------
    Creates a new Task object. Think of this as preparing a package to be sent.
    It hasn't run yet—you're just defining WHAT you want to do safely.

    PARAMS:
        - actionFunction: A function containing your risky code (e.g., DataStore, HTTP)
        
    RETURNS:
        - A new TaskStore object
]]
function TaskStore.new(actionFunction: () -> any)
	local self = setmetatable({}, TaskStore)

	self._action = actionFunction
	self.Status = "Pending" :: TaskStatus
	self.Result = nil
	self.Error = nil
	self._thenCallbacks = {}
	self._catchCallback = nil
	self._finallyCallback = nil
	self._cancelled = false

	return self
end

--[[
    Internal: runWithTimeout
    -----------------------------------------------------------------------------
    Runs a function in a separate thread and polls for completion.
    If it exceeds the timeout, the thread is cancelled and a timeout error is returned.
    This prevents hanging functions (e.g., a stuck HTTP request) from blocking forever.
]]
local function runWithTimeout(fn: () -> any, timeoutSeconds: number): (boolean, any)
	local completed = false
	local cancelled = false
	local pcallSuccess, pcallResult

	local workerThread = task.spawn(function()
		pcallSuccess, pcallResult = pcall(fn)
		if not cancelled then
			completed = true
		end
	end)

	local elapsed = 0
	local interval = 0.1
	while not completed and elapsed < timeoutSeconds do
		task.wait(interval)
		elapsed += interval
	end

	if completed then
		return pcallSuccess, pcallResult
	else
		-- Mark as cancelled so the worker thread knows not to write results
		cancelled = true
		-- Try to cancel the runaway thread (wrapped in pcall for safety)
		pcall(function()
			task.cancel(workerThread)
		end)
		return false, "Timeout: function exceeded " .. tostring(timeoutSeconds) .. " seconds"
	end
end

--[[
    Task:Await(maxRetries, delayBetween, timeout)
    -----------------------------------------------------------------------------
    This is what actually fires the pcall. It will pause (yield) the running 
    thread until the function either succeeds or runs out of retries.

    PRO-TIP: It uses Exponential Backoff! If delayBetween is 1 second:
        - Attempt 1 fails -> Waits 1 second.
        - Attempt 2 fails -> Waits 2 seconds.
        - Attempt 3 fails -> Waits 4 seconds.
        - Attempt 4 fails -> Waits 8 seconds.
    This gives shaky servers time to breathe and recover.

    PARAMS:
        - maxRetries: (number, optional) How many times to try before giving up. Default is 1.
        - delayBetween: (number, optional) Base wait time in seconds between retries. Default is 1.
        - timeout: (number, optional) Max seconds per attempt before cancelling. Default is nil (no timeout).

    RETURNS:
        - success: (boolean) Did it eventually succeed?
        - result: (any) The returned data if success is true, or the final error message if false.
]]
function TaskStore:Await(maxRetries: number?, delayBetween: number?, timeout: number?)
	maxRetries = maxRetries or 1
	delayBetween = delayBetween or 1

	local attempts = 0
	local success = false

	while attempts < maxRetries and not success and not self._cancelled do
		attempts += 1

		-- Run the risky function — with or without timeout
		if timeout then
			success, self.Result = runWithTimeout(self._action, timeout)
		else
			success, self.Result = pcall(self._action)
		end

		if not success then
			self.Error = self.Result -- On failure, pcall returns the error string
			self.Result = nil

			-- If we have attempts left and not cancelled, wait before trying again
			if attempts < maxRetries and not self._cancelled then
				local backoffTime = delayBetween * (2 ^ (attempts - 1))
				warn(string.format("[TaskStore] Attempt %d failed. Retrying in %ds. Error: %s", attempts, backoffTime, tostring(self.Error)))
				task.wait(backoffTime)
			end
		end
	end

	-- Finalize the task state (skip if cancelled before completing)
	if self._cancelled then
		self.Status = "Rejected"
		self.Error = "Task was cancelled"
	elseif success then
		self.Status = "Resolved"

		-- Fire all Then callbacks registered before Await
		for _, cb in ipairs(self._thenCallbacks) do
			local cbSuccess, cbErr = pcall(cb, self.Result)
			if not cbSuccess then
				warn("[TaskStore] Then callback errored: " .. tostring(cbErr))
			end
		end
	else
		self.Status = "Rejected"
		warn(string.format("[TaskStore] Task completely failed after %d attempts. Final Error: %s", maxRetries, tostring(self.Error)))

		-- Fire Catch callback if registered before Await
		-- Upgraded: We capture the fallback result to cleanly resolve the chain on catch!
		if self._catchCallback then
			local cbSuccess, cbErr = pcall(self._catchCallback, self.Error)
			if cbSuccess then
				self.Result = cbErr
				self.Error = nil
				self.Status = "Resolved"
				success = true
			else
				warn("[TaskStore CRITICAL] Catch callback errored: " .. tostring(cbErr))
			end
		end
	end

	-- Fire Finally callback if registered before Await
	if self._finallyCallback then
		local cbSuccess, cbErr = pcall(self._finallyCallback, self.Status)
		if not cbSuccess then
			warn("[TaskStore] Finally callback errored: " .. tostring(cbErr))
		end
	end

	return success, self.Result or self.Error
end

--[[
    Task:Then(successCallback)
    -----------------------------------------------------------------------------
    Runs a callback function ONLY if the task succeeded (Status == "Resolved").
    The result data is passed to your callback.

    Can be called BEFORE :Await() (stored, fires after Await succeeds)
    or AFTER :Await() (fires immediately if already Resolved).

    CHAINING: Returns self so you can do task:Then(...):Catch(...):Finally(...)

    PARAMS:
        - successCallback: A function that receives the result data.

    RETURNS:
        - self (for chaining)
]]
function TaskStore:Then(successCallback: (result: any) -> ()): any
	table.insert(self._thenCallbacks, successCallback)

	-- If already resolved, fire immediately
	if self.Status == "Resolved" then
		local cbSuccess, cbErr = pcall(successCallback, self.Result)
		if not cbSuccess then
			warn("[TaskStore] Then callback errored: " .. tostring(cbErr))
		end
	end

	return self
end

--[[
    Task:Catch(fallbackFunction)
    -----------------------------------------------------------------------------
    If the task completely failed (Status == "Rejected"), this runs an 
    alternative chunk of code to prevent your game from breaking.

    USE CASE: If DataStores fail to load a player's inventory, use :Catch() to 
    give them basic default starter items so they can still play the game.

    PARAMS:
        - fallbackFunction: A function that accepts the error message and returns fallback data.

    CHAINING: Returns self so you can do task:Then(...):Catch(...):Finally(...)

    CAN BE CALLED:
        - BEFORE :Await() — stored, fires after Await fails.
        - AFTER :Await() — fires immediately if already Rejected.

    PARAMS:
        - fallbackFunction: A function that accepts the error message.

    RETURNS:
        - self (for chaining)
]]
function TaskStore:Catch(fallbackFunction: (err: string) -> any): any
	self._catchCallback = fallbackFunction

	-- If already rejected, fire immediately
	if self.Status == "Rejected" then
		local cbSuccess, cbErr = pcall(fallbackFunction, self.Error)
		if cbSuccess then
			self.Result = cbErr
			self.Error = nil
			self.Status = "Resolved"
		else
			warn("[TaskStore CRITICAL] Catch callback errored: " .. tostring(cbErr))
		end
	end

	return self
end

--[[
    Task:Finally(cleanupCallback)
    -----------------------------------------------------------------------------
    Runs a callback function REGARDLESS of whether the task succeeded or failed.
    The Status string ("Resolved" or "Rejected") is passed to your callback.

    Can be called BEFORE :Await() (stored, fires after Await completes)
    or AFTER :Await() (fires immediately since Status is already set).

    CHAINING: Returns self so you can do task:Then(...):Catch(...):Finally(...)

    USE CASE: Clean up resources, close connections, hide loading screens,
    or log telemetry — no matter what happened.

    PARAMS:
        - cleanupCallback: A function that receives the task Status string.

    RETURNS:
        - self (for chaining)
]]
function TaskStore:Finally(cleanupCallback: (status: TaskStatus) -> ()): any
	self._finallyCallback = cleanupCallback

	-- If already finished, fire immediately
	if self.Status ~= "Pending" then
		local cbSuccess, cbErr = pcall(cleanupCallback, self.Status)
		if not cbSuccess then
			warn("[TaskStore] Finally callback errored: " .. tostring(cbErr))
		end
	end

	return self
end

--[[
    Task:Cancel()
    -----------------------------------------------------------------------------
    Marks the task as cancelled. If :Await() is currently running (e.g., waiting
    between retry attempts), it will stop retrying and set Status to "Rejected".

    USE CASE: TaskStore.Any() calls this on losing tasks after the first task
    succeeds, so they don't keep consuming resources.
]]
function TaskStore:Cancel()
	self._cancelled = true
end

--[[
    TaskStore.Any(tasks, ...)  [STATIC]
    -----------------------------------------------------------------------------
    Runs multiple tasks CONCURRENTLY and returns the FIRST one that succeeds.
    Think of this like Promise.race() — but it waits for a success, not just
    the first to finish (failures don't count).

    USE CASE: Query multiple data sources and use whichever responds first.

    PARAMS:
        - tasks: (table) Array of TaskStore objects.
        - ...: Extra args forwarded to each task's :Await() (maxRetries, delayBetween, timeout).

    RETURNS:
        - The first successful task object, or nil if ALL tasks failed.
]]
function TaskStore.Any(tasks: {any}, ...: any): any?
	local args = table.pack(...)
	local completed = 0
	local total = #tasks
	local firstSuccess = nil

	local runningThread = coroutine.running()
	local threadResumed = false

	for _, taskObj in ipairs(tasks) do
		task.spawn(function()
			local success = taskObj:Await(table.unpack(args))
			completed += 1

			if success and not firstSuccess then
				firstSuccess = taskObj
				-- Cancel all remaining tasks so they don't keep running
				for _, otherTask in ipairs(tasks) do
					if otherTask ~= taskObj then
						otherTask:Cancel()
					end
				end
				if not threadResumed then
					threadResumed = true
					task.spawn(runningThread)
				end
			elseif completed == total then
				if not threadResumed then
					threadResumed = true
					task.spawn(runningThread)
				end
			end
		end)
	end

	if completed < total and not firstSuccess then
		coroutine.yield()
	end

	return firstSuccess
end

--[[
    TaskStore.All(tasks, ...)  [STATIC]
    -----------------------------------------------------------------------------
    Runs multiple tasks CONCURRENTLY and waits for ALL of them to finish.
    Think of this like Promise.all().

    USE CASE: Load multiple DataStore keys at once (player data, inventory, settings).

    PARAMS:
        - tasks: (table) Array of TaskStore objects.
        - ...: Extra args forwarded to each task's :Await() (maxRetries, delayBetween, timeout).

    RETURNS:
        - An array of tables: { {success, result, task}, ... } for each task in order.
]]
function TaskStore.All(tasks: {any}, ...: any): {any}
	local args = table.pack(...)
	local results = table.create(#tasks)
	local completed = 0
	local total = #tasks

	local runningThread = coroutine.running()
	local threadResumed = false

	for i, taskObj in ipairs(tasks) do
		task.spawn(function()
			local success, result = taskObj:Await(table.unpack(args))
			results[i] = {
				success = success,
				result = result,
				task = taskObj,
			}
			completed += 1

			if completed == total then
				if not threadResumed then
					threadResumed = true
					task.spawn(runningThread)
				end
			end
		end)
	end

	if completed < total then
		coroutine.yield()
	end

	return results
end

-- Module loaded successfully
return TaskStore
