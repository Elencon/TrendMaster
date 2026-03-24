import sys

print("Diagnostic starting...")
sys.path.insert(0, r"c:\Economy\Invest\TrendMaster\src")
print(f"Added src to sys.path: {sys.path[0]}")

print("Importing config...", end=" ", flush=True)
print("SUCCESS")

print("Importing connect...", end=" ", flush=True)
print("SUCCESS")

print("Diagnostic finished.")
