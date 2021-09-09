#%%
import json
import os
condition_name = {
    'domain': 'ours',
    'graph': 'subgraph',
    'model': 'neighbor nodes',
    'baseline': 'baseline'
}


GTs = [
    # // unipolar depression
    # // = Desipramine, true
    { "disease": '5263.0', "drug": 'DB01151', "note": True},
    # // = Paroxetine, true
    { "disease": '5263.0', "drug": 'DB00715', "note": True},
    # // = Trazodone, true
    { "disease": '5263.0', "drug": 'DB00656', "note": True},
    # // = Imipramine, true
    { "disease": '5263.0', "drug": 'DB00458', "note": True},
    # //
    # // cornary artery disease
    # // Urokinase, true?
    { "disease": '5010.0', "drug": 'DB00013', "note": True},
    # // Dalfampridine, false
    { "disease": '5010.0', "drug": 'DB06637', "note": False},
    # // Febuxostat, false
    { "disease": '5010.0', "drug": 'DB04854', "note": False},
    # // Dihydroergocornine, studied long time ago
    { "disease": '5010.0', "drug": 'DB11273', "note": True},
    # // colorectal cancer
    {
    #   // colorectal cancer, Dexamethasone, in trial
      "disease": '5575.0', "drug": 'DB01234', "note": True
    },
    {
    #   // colorectal cancer, Cisplatin, true
      "disease": '5575.0', "drug": 'DB00515', "note": True
    },
    {
    #   // colorectal cancer, Rucaparib, in trial
      "disease": '5575.0', "drug": 'DB12332', "note": True
    },
    {
    #   // colorectal cancer, Gemtuzumab ozogamicin, false
      "disease": '5575.0', "drug": 'DB00056', "note": False
    },
    {
    #   // alzheimer, Rivastigmine, true
      "disease":
        '4975_7088_7089_11194_11561_11647_11777_12321_12344_12609_12630_12631_12632',
      "drug": 'DB00989', "note": True
    },
    {
    #   // alzheimer, Galantamine, true
      "disease":
        '4975_7088_7089_11194_11561_11647_11777_12321_12344_12609_12630_12631_12632',
      "drug": 'DB00674' , "note": True
    },
    {
    #   // alzheimer, // Orphenadrine, false
      "disease":
        '4975_7088_7089_11194_11561_11647_11777_12321_12344_12609_12630_12631_12632',
      "drug": 'DB01173', "note": False
    },
    {
    #    alzheimer, Pioglitazone, in trial
      "disease":
        '4975_7088_7089_11194_11561_11647_11777_12321_12344_12609_12630_12631_12632',
      "drug": 'DB01132', "note": True
    },
  ]

#%%
metric = ['understand', 'trust', 'helpful', 'willToUse']
conditions = ['domain', 'graph', 'model', 'baseline']
levels = ['strongly disagree', 'disagree', 'neutral' , 'agree', 'strongly agree']

results = {}
for m in metric:
    results[m] = {}
    for c in conditions:
        results[m][c] = [0 for _ in levels]

for filename in os.listdir('./'):
    if filename.endswith('.json') and not 'results' in filename:
        with open(f'./{filename}') as f:
            content = json.load(f)
            rating = content['answers'][22]
            for m in metric:
                for c in conditions:
                    index = int(rating[f'overall_{m}_{c}'])
                    results[m][c][index+2] += 1

with open('./result/overall_results.json', 'w') as f:
    json.dump(results, f)


with open(f'./result/overall_results.csv', 'w') as f:
    csvwriter = csv.writer(f)
    for m in metric:
        csvwriter.writerow([m])
        csvwriter.writerow(['Question']+ levels)
        for c in conditions:
            csvwriter.writerow([condition_name[c]]+ [results[m][c][i] for i in range(len(levels))])

for m in metric:
    with open(f'./chart/{m}_results.csv', 'w') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerow(['Question']+ levels)
        for c in conditions:
            csvwriter.writerow([condition_name[c]]+ [results[m][c][i] for i in range(len(levels))])

# %%
# agree or not agree
agree_results = {}
accuracy_results = {}
confidence_results = {}
confidence_results_2  = {'agree': {}, 'not_agree': {}}
   

for c in conditions:
    agree_results[c] = [0, 0] # no, yes
    accuracy_results[c] = [0,0] # wrong, right
    confidence_results[c] = [0 for _ in range(5)]
    confidence_results_2['agree'][c] = [0 for _ in range(5)]
    confidence_results_2['not_agree'][c] = [0 for _ in range(5)]



for filename in os.listdir('./'):
    if filename.endswith('.json') and not 'results' in filename:
        with open(f'./{filename}') as f:
            content = json.load(f)
            ratings = content['answers'][6:22]
            questions = content['questions']
            for i in range(16):
                condition = questions[i]['condition']
                answer = ratings[i][f"task_question_{i}"]
                for GT in GTs:
                    if GT['disease'] == questions[i]['disease'] and GT['drug'] == questions[i]['drug']:
                        if GT['note']==True and answer == 'indicatable':
                            accuracy_results[condition][1] += 1
                        elif GT['note']==False and answer == 'not indicatable':
                            accuracy_results[condition][1] += 1
                        else:
                            accuracy_results[condition][0] += 1
                        break
                confidence = int(ratings[i][f"task_confidence_{i}"])
                if answer == 'indicatable':
                    agree_results[condition][1] += 1
                    confidence_results_2['agree'][condition][confidence+2] += 1
                else: 
                    agree_results[condition][0] += 1
                    confidence_results_2['not_agree'][condition][confidence+2] += 1
                confidence_results[condition][confidence+2] += 1
                
            
with open('./result/agree_results.json', 'w') as f:
    json.dump(agree_results, f)

with open('./result/acc_results.json', 'w') as f:
    json.dump(accuracy_results, f)
with open('./result/confidence_results.json', 'w') as f:
    json.dump(confidence_results, f)
with open('./result/confidence_results_2.json', 'w') as f:
    json.dump(confidence_results_2, f)
#%%
with open('./chart/confidence_results.csv', 'w') as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow(['Question'] + levels)
    for c in conditions:
        csvwriter.writerow([condition_name[c]] + [c/32 for c in confidence_results[c]])
#%%
# age and experience
ages  = []
exps = []
for filename in os.listdir('./'):
    if filename.endswith('.json') and not 'results' in filename:
        with open(f'./{filename}') as f:
            content = json.load(f)
            ages.append(content['answers'][0]['age'])
            exps.append(content['answers'][0]['bio'])

print(ages)
#%%
# anova analysis
import csv
results = {}
for m in metric:
    results[m] = {}
    for c in conditions:
        results[m][c] = []

for filename in os.listdir('./'):
    if filename.endswith('.json') and not 'results' in filename:
        with open(f'./{filename}') as f:
            content = json.load(f)
            rating = content['answers'][22]
            for m in metric:
                for c in conditions:
                    index = int(rating[f'overall_{m}_{c}'])
                    results[m][c].append(index+3)

with open('./result_anova/overall_results.csv', 'w') as f:
    csvwriter = csv.writer(f)
    for m in metric:
        csvwriter.writerow([m]+[ condition_name[c] for c in conditions])
        for participant in range(8):
            csvwriter.writerow([f'p_{participant}']+ [results[m][c][participant] for c in conditions])
    
# %% anova
import csv
accuracy_results = {}
agree_results = {}
confidence_res = {}
confidence_2 = {'agree':{},'not_agree': {} }
for c in conditions:
    accuracy_results[c] = [0 for _ in range(8)]
    agree_results[c] = [0 for _ in range(8)]
    confidence_res[c] = [ ]
    confidence_2['agree'][c]=[]
    confidence_2['not_agree'][c]=[]
participant = 0
for filename in os.listdir('./'):
    if filename.endswith('.json') and not 'results' in filename:
        
        with open(f'./{filename}') as f:
            content = json.load(f)
            ratings = content['answers'][6:22]
            questions = content['questions']
            for i in range(16):
                condition = questions[i]['condition']
                answer = ratings[i][f"task_question_{i}"]
                for GT in GTs:
                    if GT['disease'] == questions[i]['disease'] and GT['drug'] == questions[i]['drug']:
                        if GT['note']==True and answer == 'indicatable':
                            accuracy_results[condition][participant] += 1
                        elif GT['note']==False and answer == 'not indicatable':
                            accuracy_results[condition][participant] += 1                 
                        break
                confidence = int(ratings[i][f"task_confidence_{i}"])
                if answer == 'indicatable':
                    confidence_2['agree'][condition].append(confidence+3)
                    agree_results[condition][participant] += 1
                else: 
                    confidence_2['not_agree'][condition].append(confidence+3)
                confidence_res[condition].append(confidence+3)

        participant += 1

for c in conditions:
    accuracy_results[c] = [d/4 for d in accuracy_results[c]]

for c in conditions:
    agree_results[c] = [d/4 for d in agree_results[c]]

# %%
with open('./result_anova/confidence_results_2.json', 'w') as f:
    json.dump(confidence_2, f)

with open('./result_anova/confidence_results.json', 'w') as f:
    json.dump(confidence_res, f)
# %%
