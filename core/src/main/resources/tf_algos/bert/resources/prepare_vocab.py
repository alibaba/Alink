import os
import shutil
import sys
import tempfile

from transformers import AutoTokenizer

models = [
    'bert-base-cased',
    'bert-base-uncased',
    'bert-base-multilingual-cased',
    'bert-base-chinese'
]

prefix = os.path.dirname(sys.argv[0])

for model in models:
    tokenizer = AutoTokenizer.from_pretrained(model)
    with tempfile.TemporaryDirectory() as tempdirname:
        tokenizer.save_pretrained(tempdirname)
        shutil.make_archive(os.path.join(prefix, model + "-vocab"), format="gztar", root_dir=tempdirname)
