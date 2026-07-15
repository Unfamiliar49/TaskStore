# TaskStore! 
An upgraded, state-driven `pcall` and asynchronous task runner framework built specifically for Roblox (Luau).

## Features
* **State Tracking:** Keeps track of tasks using `"Pending"`, `"Resolved"`, and `"Rejected"` states.
* **Auto-Retries with Exponential Backoff:** Gracefully retries failed requests, scaling the delay between attempts to prevent server spam.
* **Timeout Protection:** Abort runaway HTTP requests or hanging API queries.
* **Callback Chaining:** Chain callbacks seamlessly with `.Then()`, `.Catch()`, and `.Finally()`.
* **Parallel Concurrency:** Wait for multiple tasks using `TaskStore.All()` or race them and auto-cancel losers using `TaskStore.Any()`.

## Installation
1. Copy the source code from `src/TaskStore.lua`.
2. Create a `ModuleScript` named `TaskStore` inside `ReplicatedStorage` in Roblox Studio.
3. Paste the code into the ModuleScript.
4. Or find it in the Creator Store (have to enable See Unverified creator) 
