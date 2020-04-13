import pickle
from datetime import datetime

def get_paths(checkpoint_dir, checkpoint_name):
  meta_full_path = checkpoint_dir / (checkpoint_name + '_meta.pkl')
  pkl_full_path = checkpoint_dir / (checkpoint_name + '.pkl')
  return meta_full_path, pkl_full_path

def get_collection_timestamp(config, path):
  meta_full_path, pkl_full_path = get_paths(config.dir, path)
  with meta_full_path.open('rb') as file:
    meta_data = pickle.load(file)
    return meta_data['created']

def get_is_expired(config, path):
  try:
    get_collection_timestamp(config, path)
    return False
  except FileNotFoundError:
    return True

def should_expire(config, path, expire_fn):
  return expire_fn(get_collection_timestamp(config, path))

def store_data(config, path, data):
  created = datetime.now()
  meta_data = {'created': created}
  meta_full_path, pkl_full_path = get_paths(config.dir, path)
  pkl_full_path.parent.mkdir(parents=True, exist_ok=True)
  with pkl_full_path.open('wb') as file:
    pickle.dump(data, file, -1)
  with meta_full_path.open('wb') as file:
    pickle.dump(meta_data, file, -1)
  return data

def load_data(config, path):
  _, full_path = get_paths(config.dir, path)
  with full_path.open('rb') as file:
    return pickle.load(file)

def delete_data(config, path):
  meta_full_path, pkl_full_path = get_paths(config.dir, path)
  try:
    meta_full_path.unlink()
    pkl_full_path.unlink()
  except FileNotFoundError:
    pass
