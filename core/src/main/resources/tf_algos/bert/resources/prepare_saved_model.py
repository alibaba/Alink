import os
import sys

from transformers import TFBertModel

models = [
    'bert-base-cased',
    'bert-base-uncased',
    'bert-base-multilingual-cased',
    'bert-base-chinese'
]

prefix = os.path.dirname(sys.argv[0])

for model in models:
    m = TFBertModel.from_pretrained(model, output_hidden_states=True)
    m.save_pretrained(os.path.join(prefix, model + "-savedmodel"), saved_model=True)
