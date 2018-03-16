from .function_body import get_function_hash
from relib import get_tmp_directory, DirectoryBytesIO
from functools import partial

class PickleableTf:
  def __init__(self, get_model_funcs, model_funcs_names=None, bytes=None):
    if model_funcs_names == None:
      import tensorflow as tf
      try:
        tf.reset_default_graph()
      except AssertionError:
        pass
      model_funcs_names = sorted(get_model_funcs().keys())

    self.model_funcs_names = model_funcs_names
    self.get_model_funcs = get_model_funcs
    self.get_model_funcs_hash = get_function_hash(get_model_funcs)
    self.bytes = bytes

    for func_name in model_funcs_names:
      compute = partial(self.compute, func_name)
      setattr(self, func_name, compute)

  def compute(self, func_name, *args, **kwargs):
    import tensorflow as tf

    load_tmp_directory = get_tmp_directory()
    save_tmp_directory = get_tmp_directory()
    file_name = 'tf.ckpt'

    tf.reset_default_graph()
    model_funcs = self.get_model_funcs()
    saver = tf.train.Saver()

    with tf.Session() as sess:
      if self.bytes:
        self.bytes.unpack(load_tmp_directory)
        saver.restore(sess, load_tmp_directory + file_name)
      else:
        init_op = tf.global_variables_initializer()
        sess.run(init_op)

      result = model_funcs[func_name](sess, *args, **kwargs)
      saver.save(sess, save_tmp_directory + file_name)
      bytes = DirectoryBytesIO(save_tmp_directory)
      model = PickleableTf(self.get_model_funcs, self.model_funcs_names, bytes)
      return model, result
