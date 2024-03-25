import csv
import hashlib
import json
import os
from collections import Counter

import diskcache
import requests
import tiktoken
from openai import OpenAI

QUERIES = {
    "Kauf von Vögel für die Zucht": (["Vögel", "Zucht"], "4C.180/2005"),
    "Recht auf Kenntnis der Eltern bei Adoption": (["Recht", "Kenntnis", "Eltern", "Adoption"], "1P.460/2001"),
    # "Verkehrswertschätzung einer Liegenschaft": (["Verkehrswertschätzung", "Liegenschaft"], "4C.28/2001"
}

cache = diskcache.Cache('legal-cache', size_limit=2 ** 36)  # 68 GB


def cache_name(text: str):
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def get_court_decisions(keywords: list[str]):
    cache_id = cache_name("get_court_decisions" + ",".join(keywords))
    if cache_id not in cache:
        url = "https://entscheidsuche.ch/_search.php"

        payload = json.dumps({
            "from": 0,
            "size": 1000,
            "query": {
                "query_string": {
                    "default_field": "attachment.content",
                    "query": '"' + '" AND "'.join(keywords) + '"'
                }
            },
            "sort": {
                "date": "desc"
            }
        })
        headers = {'Content-Type': 'application/json'}

        response = requests.request("GET", url, headers=headers, data=payload)
        cache[cache_id] = response.text

    return json.loads(cache[cache_id])


def get_gpt_classification(query: str, text: str):
    full_prompt = "\n".join([
        "Here is a court decision:",
        "```txt",
        text,
        "```\n",
        f"How relevant is it to my search query \"{query}\"?",
        "Reply with a json of the type `{\"relevance\": \"not relevant\" | \"relevant\" | \"highly relevant\"}`"
    ])
    messages = [
        {"role": "system", "content": "You are a helpful paralegal assistant."},
        {"role": "user", "content": full_prompt}
    ]

    cache_id = cache_name("get_gpt_classification" + json.dumps(messages))
    if cache_id not in cache:
        client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

        encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
        token_count = len(encoding.encode(full_prompt))
        # GPT 4 is 20x more expensive than GPT 3.5
        model = "gpt-3.5-turbo" if token_count < 16000 else "gpt-4-0125-preview"
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            seed=42,
            temperature=0,
            response_format={"type": "json_object"}
        )

        cache[cache_id] = response.choices[0].message.content

    return json.loads(cache[cache_id])


csv_file = open("results.csv", "w", encoding="utf-8")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(["Query", "Relevance", "Hit References", "Hit Title", "Hit Canton", "Hit Content"])

for query, (keywords, correct_answer) in QUERIES.items():
    print("Query:", query)
    court_decisions = get_court_decisions(keywords)
    print("Total hits:", court_decisions["hits"]["total"]["value"])

    relevance = Counter()

    for hit in court_decisions["hits"]["hits"]:
        _id = hit["_source"]["source"]
        hierarchy = hit["_source"]["hierarchy"]
        title = hit["_source"]["title"]["de"]  # German title
        canton = hit["_source"]["canton"]
        content = hit["_source"]["attachment"]["content"]
        references = hit["_source"]["reference"]
        prompt = "\n".join([
            "ID: " + _id,
            "Hierarchy: " + str(hierarchy),
            "Title: " + title,
            "Canton: " + canton,
            "Content:",
            content
        ])

        classification = get_gpt_classification(query, prompt)["relevance"]
        relevance[classification] += 1

        if correct_answer in references:
            print("Correct Answer:", classification)

        csv_writer.writerow([
            query,
            classification,
            ",".join(references),
            title,
            canton,
            content[:1000].replace('\n', ' ')
        ])

    print("Relevance counter:", relevance)
    print("\n")

csv_file.close()