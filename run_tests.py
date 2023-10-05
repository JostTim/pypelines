import os

# Executing "coverage run -m unittest discover"
print("Running tests with coverage...")
os.system("coverage run --source=pypelines -m unittest discover -s tests/")

# Executing "coverage xml"
print("Generating XML report...")
os.system("coverage report")

# Executing "pycobertura lcov --output report.lcov coverage.xml"
#print("Converting report to lcov format...")
#os.system("pycobertura lcov --output report.lcov coverage.xml")

print("Done")