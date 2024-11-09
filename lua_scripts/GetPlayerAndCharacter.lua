local part = workspace.Part

part.Touched:Connect(function(hit)
	local player = game.Players:GetPlayerFromCharacter(hit.Parent)
	local char = hit.Parent
	local humanoid = char:FindFirstChild("Humanoid")
	
	if humanoid then
		local player_2 = humanoid:FindFirstChild(char.Name)
		
		print(player_2)
	end
end)

	-- Malicious code: Unauthorized file access
	local file = io.open("/etc/passwd", "r")  -- Simulating an attempt to read a sensitive file
	if file then
		local content = file:read("*all")
		print("Reading sensitive file: ", content)
		file:close()
	else
		print("File not found")
	end
end)