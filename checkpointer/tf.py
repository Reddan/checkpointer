from functools import partial
from relib import DirectoryBytesIO
from checkpointer import get_function_hash

def ensure_list(val):
  if isinstance(val, (tuple, list)):
    return list(val)
  else:
    return [val]

class PickleableTf:
  def __init__(self, get_model_funcs, *args, **kwargs):
    model_funcs_names = kwargs.get('model_funcs_names', None)
    bytes = kwargs.get('bytes', None)

    if 'model_funcs_names' in kwargs:
      del kwargs['model_funcs_names']
      del kwargs['bytes']

    if model_funcs_names == None:
      import tensorflow as tf

      with tf.Graph().as_default():
        model_funcs = ensure_list(get_model_funcs(*args, **kwargs))
        model_funcs_names = [func.__name__ for func in model_funcs]

    self.args = args
    self.kwargs = kwargs
    self.model_funcs_names = model_funcs_names
    self.get_model_funcs = get_model_funcs
    self.get_model_funcs_hash = get_function_hash(get_model_funcs)
    self.bytes = bytes

    for func_name in model_funcs_names:
      compute = partial(self._compute, func_name)
      setattr(self, func_name, compute)

  def _compute(self, func_name, *args, **kwargs):
    import tensorflow as tf

    with tf.Graph().as_default():
      model_funcs = {
        func.__name__: func
        for func in ensure_list(self.get_model_funcs(*self.args, **self.kwargs))
      }

      ckpt_file_name = 'tf.ckpt'
      saver = tf.train.Saver()

      with tf.Session() as sess:
        bytes = self.bytes

        def save():
          nonlocal bytes
          bytes = DirectoryBytesIO(lambda tmp_directory: \
            saver.save(sess, tmp_directory + ckpt_file_name)
          )

        if self.bytes:
          self.bytes.unpack(lambda tmp_directory: \
            saver.restore(sess, tmp_directory + ckpt_file_name)
          )
        else:
          init_op = tf.global_variables_initializer()
          sess.run(init_op)

        result = model_funcs[func_name](sess, save, *args, **kwargs)
        model = PickleableTf(
          self.get_model_funcs,
          *self.args,
          **self.kwargs,
          model_funcs_names=self.model_funcs_names,
          bytes=bytes
        )
        return model, result
