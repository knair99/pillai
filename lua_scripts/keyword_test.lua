-- This Lua script demonstrates potentially malicious behavior

-- Infinite loop that could cause a denial of service
while true do
    print("This loop will run forever, consuming resources.")
end

-- Attempt to execute a shell command, which can be used for malicious purposes
local handle = io.popen("rm /Desktop/test")  -- Be cautious with such commands!
local result = handle:read("*a")
handle:close()

print("Executed shell command: rm /Desktop/test")

