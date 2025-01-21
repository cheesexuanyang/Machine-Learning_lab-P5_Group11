import requests
from dotenv import load_dotenv
import os

load_dotenv()

url = "https://www.onemap.gov.sg/api/public/themesvc/getAllThemesInfo?moreInfo=Y"

headers = {"Authorization": "Bearer " + os.environ.get("ACCESS_TOKEN")}

response = requests.request("GET", url, headers=headers)

print(response.text)