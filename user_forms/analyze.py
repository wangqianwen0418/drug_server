#%%
import json
import os

#%%
metric = ['understand', 'trust', 'helpful', 'willToUse']
conditions = ['baseline', 'domain', 'graph', 'model']

results = {}
for m in metric:
    results[m] = {}
    for c in conditions:
        results[m][c] = [0 for _ in range(5)]

for filename in os.listdir('./'):
    if filename.endswith('.json') and not 'results' in filename:
        with open(f'./{filename}') as f:
            content = json.load(f)
            rating = content['answers'][22]
            for m in metric:
                for c in conditions:
                    index = int(rating[f'overall_{m}_{c}'])
                    results[m][c][index+2] += 1

with open('./overall_results.json', 'w') as f:
    json.dump(results, f)

# %%
# agree or not agree
agree_results = {}
confidence_results = {}
confidence_results_2  = {'agree': {}, 'not_agree': {}}
for c in conditions:
    agree_results[c] = [0, 0] # no, yes

for c in conditions:
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
                confidence = int(ratings[i][f"task_confidence_{i}"])
                if answer == 'indicatable':
                    agree_results[condition][1] += 1
                    confidence_results_2['agree'][condition][confidence+2] += 1
                else: 
                    agree_results[condition][0] += 1
                    confidence_results_2['not_agree'][condition][confidence+2] += 1
                confidence_results[condition][confidence+2] += 1
                
            
with open('./agree_results.json', 'w') as f:
    json.dump(agree_results, f)
with open('./confidence_results.json', 'w') as f:
    json.dump(confidence_results, f)
with open('./confidence_results_2.json', 'w') as f:
    json.dump(confidence_results_2, f)
#%%
# confidence level