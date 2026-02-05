from google.cloud import storage

client = storage.Client()
bucket = client.bucket('coffeespace-sandbox-source-1')
blob = bucket.blob('CoffeeSpaceTestDatav4.jsonl')

# Download to local
blob.download_to_filename('./local-file.jsonl')

# # Or read directly into memory (for smaller files)
# content = blob.download_as_text()


pip install ijson  # streaming JSON parser

gsutil cat gs://coffeespace-sandbox-source-2/Linkedin_Sample_Data.1.json |
python3 -c "
import ijson, json, sys
for i, item in enumerate(ijson.items(sys.stdin.buffer,
'item')):
    if i >= 2: break
    print(json.dumps(item, indent=2))
"