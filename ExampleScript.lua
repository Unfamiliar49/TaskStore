-- 1. Grab the module (Assuming it's in ReplicatedStorage)
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local TaskStore = require(ReplicatedStorage:WaitForChild("TaskStore"))

--=================================================================================
-- EXAMPLE 1: Await + Then + Catch (the classic pattern, now chained!)
--=================================================================================

local loadDataTask = TaskStore.new(function()
	-- Simulating a data fetch that randomly fails 70% of the time
	if math.random() > 0.3 then
		error("Database throttled - rate limit exceeded!")
	end
	return { Coins = 250, Inventory = {"Wood Plank"} }
end)

-- Chain callbacks BEFORE calling Await — they fire automatically after Await completes
loadDataTask
	:Then(function(result)
		print("Successfully loaded player data:", result.Coins, "coins.")
	end)
	:Catch(function(errMessage)
		print("Using local backup save because of error:", errMessage)
		-- NEW FEATURE: Returning a value from Catch now recovers the chain!
		-- This returned table automatically becomes the task's final .Result
		return { Coins = 0, Inventory = {} } 
	end)
	:Finally(function(status)
		-- Since Catch recovered the failure, status will be "Resolved" even if the main action failed!
		-- We can safely read our final data directly from the task
		local finalData = loadDataTask.Result
		print("Player logged in. Final Coin Count:", finalData.Coins)
	end)

-- Await with 3 retries, 1-second base backoff delay
loadDataTask:Await(3, 1)

--=================================================================================
-- EXAMPLE 2: Chaining with :Then() and :Finally()
--=================================================================================

local chainedTask = TaskStore.new(function()
	return { Level = 42, XP = 1500 }
end)

-- Chain callbacks BEFORE calling Await — they fire automatically after Await completes
chainedTask
	:Then(function(result)
		print("[Chain] Data loaded! Level:", result.Level, "XP:", result.XP)
	end)
	:Catch(function(errMessage)
		print("[Chain] Data load failed, using defaults. Error:", errMessage)
	end)
	:Finally(function(status)
		print("[Chain] Cleanup finished. Final status:", status)
	end)

chainedTask:Await(2, 1)

--=================================================================================
-- EXAMPLE 3: Timeout Support
--=================================================================================

-- This function simulates a hanging request that never returns
local hangingTask = TaskStore.new(function()
	task.wait(999) -- Pretend this is a stuck HTTP request
	return "This will never be reached"
end)

-- Await with a 3-second timeout per attempt — no more hanging forever!
local timeoutSuccess, timeoutResult = hangingTask:Await(2, 1, 3)

if not timeoutSuccess then
	print("[Timeout] Task failed:", timeoutResult)
end

--=================================================================================
-- EXAMPLE 4: TaskStore.Any() — First to win
--=================================================================================

-- Create 3 tasks that simulate different data sources with varying speeds
local tasks = {
	TaskStore.new(function()
		task.wait(2)
		return "Source A responded"
	end),
	TaskStore.new(function()
		task.wait(0.5)
		return "Source B responded (fastest!)"
	end),
	TaskStore.new(function()
		task.wait(1)
		return "Source C responded"
	end),
}

-- Run all concurrently, return the FIRST one that succeeds.
-- Remaining tasks are automatically cancelled once a winner is found.
local winner = TaskStore.Any(tasks, 1, 1)

if winner then
	print("[Any] First successful result:", winner.Result)
else
	print("[Any] All tasks failed.")
end

--=================================================================================
-- EXAMPLE 5: TaskStore.All() — Wait for everything
--=================================================================================

-- Create 2 tasks for loading different data keys
local allTasks = {
	TaskStore.new(function()
		task.wait(1)
		return { Coins = 500 }
	end),
	TaskStore.new(function()
		task.wait(0.5)
		return { Gems = 12 }
	end),
}

-- Run all concurrently, wait for ALL to finish
local results = TaskStore.All(allTasks, 3, 1)

for i, result in ipairs(results) do
	if result.success then
		print(string.format("[All] Task %d succeeded: %s", i, tostring(result.result)))
	else
		print(string.format("[All] Task %d failed: %s", i, tostring(result.result)))
	end
end
