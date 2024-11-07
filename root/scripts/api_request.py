import requests
import json

# Define the API URL
url = "https://api.openbrewerydb.org/breweries"

# Make the request to the API
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    breweries = response.json()  # Convert the response to JSON
    print("Data successfully retrieved!")
else:
    print("Failed to access the API:", response.status_code)
    breweries = []  # Define an empty list in case of an error


# Save the raw data to a JSON file
with open("breweries_raw.json", "w") as file:
    json.dump(breweries, file, indent=4)  # The indent parameter formats the JSON

print("Data saved to breweries_raw.json")
