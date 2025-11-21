import os
import random
import json
import shutil

# ------------------------------------------------------------
# CONFI
# ------------------------------------------------------------
projects = ["project1", "project2","project3", "project4"]
users=["a","b","c","d","e","f"]

dm3_root_folders = [
    "raw_data/108_Ag",
    "raw-data/A6-AgII_pH_7",
    "raw_data/A10-Mn_pH_7",
    "raw_data/B8-AgI_pH_7",
    "raw_data/C10-E333_pH_7"
]

output_root = "instrument_raw_data"
eln_root = "eln_data"
history_file = "history.json"


# ------------------------------------------------------------
# uopload or create the history
# ------------------------------------------------------------
if os.path.exists(history_file):
    with open(history_file, "r") as f:
        history = json.load(f)
else:
    history = { "used_files": [],
          "project1":{      
        "used_ids": []},
                 "project2":{      
        "used_ids": []},
                 "project3":{      
        "used_ids": []},
                 "project4":{      
        "used_ids": []}
    }


# ------------------------------------------------------------
#random project

project = random.choice(projects)
print("Selected project:", project)


# ------------------------------------------------------------
# random proposal
while True:
    rand_id = random.randint(100, 1000)
    if rand_id not in history[project]["used_ids"]:
        break

history[project]["used_ids"].append(rand_id)
print("Generated unique ID:", rand_id)

#random user for that proposal
user = random.choice(users)
eln={project+'-'+str(rand_id):user, "sample":[]}

# ------------------------------------------------------------
all_dm3 = []

for folder in dm3_root_folders:
    if os.path.isdir(folder):
        for file in os.listdir(folder):
            if file.lower().endswith(".dm3"):
                full_path = os.path.join(folder, file)
                if full_path not in history["used_files"]:
                    all_dm3.append(full_path)
                    
if len(all_dm3) < 4:
    raise ValueError("Too less file left")

print(eln)

# ------------------------------------------------------------
selected_files = random.sample(all_dm3, 4)

for f in selected_files:
    history["used_files"].append(f)

print("Selected DM3 files:")
for f in selected_files:

    sample_name=f.split('/')[1]
    file=f.split('/')[2]
    description="I don't know"
    index=file.lower().split("k")[0].split("-")[-1]
    additional=file.lower().split("k")[1].split("-")[-1].split("_")[-1].split('.')[0]
    sample_name_dict={sample_name:{
                        "file_name": file,
                        "description":description,
                        "index": index,
                        "additional": additional}}
    eln["sample"].append(sample_name_dict)
                    
    
    print(" -", f)
print(eln)


# ------------------------------------------------------------
dest_folder = os.path.join(output_root, project+'-'+str(rand_id))
os.makedirs(dest_folder, exist_ok=True)
eln_file=eln_root+'/'+project+'-'+str(rand_id)+'-eln.json'

with open(eln_file, "w") as f:
    json.dump(eln, f, indent=2)


# ------------------------------------------------------------
for src in selected_files:
    fname = os.path.basename(src)
    dst = os.path.join(dest_folder, fname)
    shutil.copy2(src, dst)

print(f"\nFiles copied to: {dest_folder}")



# ------------------------------------------------------------
with open(history_file, "w") as f:
    json.dump(history, f, indent=2)

print("\nHistory updated.")
