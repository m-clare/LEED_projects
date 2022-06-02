import aiohttp
import asyncio

async def get(session: aiohttp.ClientSession, id: str, address: str, **kwargs) -> dict:
    url = f"http://localhost:4000/v1/search?text={address}"
    resp = await session.request("GET", url=url, **kwargs)
    data = await resp.json()
    if data["features"]:
        if len(data["features"]) > 1:
            return {"id": id, "address": address, "features": data["features"]}
        else:
            latitude = data["features"][0]["geometry"]["coordinates"][1]
            longitude = data["features"][0]["geometry"]["coordinates"][0]
            return {
                "id": id,
                "address": address,
                "latitude": latitude,
                "longitude": longitude,
            }
    else:
        return {"id": id, "address": address, "match": "No match"}

async def main(filename, **kwargs):
    async with aiohttp.ClientSession() as session:
        file = open(filename, 'r')
        lines = file.readlines()
        tasks = []
        for i in range(len(lines)):
            try:
                id = lines[i].split(',', 1)[0]
                address = lines[i].split(',', 1)[1].strip("\n")
                tasks.append(get(session=session, id=id, address=address, **kwargs))
            except IndexError:
                pass
        htmls = await asyncio.gather(*tasks, return_exceptions=True)
        return htmls

def test_split(filename):
    file = open(filename, 'r')
    lines = file.readlines()
    for i in range(len(lines)):
        try:
            id = lines[i].split(',', 1)[0]
            address = lines[i].split(',', 1)[1].strip("\n")
            print(address)
        except IndexError:
            pass

if __name__ == "__main__":
    import sys
    import json
    filename = sys.argv[1]
    # test_split(filename)
    results = asyncio.run(main(filename))
    results_filtered = list(filter(lambda x: type(x) == dict, results))
    multiple_results = []
    for result in results_filtered:
        if result['features']:
            multiple_results.append(result)
            results_filtered.pop(result)
    with open('pelias_processed_addresses.json', 'w') as fh:
        json.dump(results_filtered, fh, indent=4)
    with open('pelias_processed_addresses_multi.json', 'w') as fh:
        json.dump(multiple_results, fh, indent=4)
