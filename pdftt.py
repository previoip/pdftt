import os
import sys
import shutil
import subprocess
import tempfile
import queue
import threading

PPLR_IMGTOPPM_ARGS = ['-r', '150', '-hide-annotations', '-gray', '-q', '-cropbox', '-aa', 'yes']
TESS_IMGTOPDF_ARGS = ['-l', 'eng+ind+jav', '--psm', '6', 'pdf']
PPLR_PDFTOTXT_ARGS = ['-layout', '-nopgbrk', '-q']

cache_dir = None

def _clean_cache_dir():
  global cache_dir
  if not cache_dir:
    return
  if os.path.exists(cache_dir) and os.path.isdir(cache_dir):
    shutil.rmtree(cache_dir)

def _generate_cache_dir(path=None):
  global cache_dir
  _clean_cache_dir()
  if path is None:
    cache_dir = tempfile.mkdtemp()
    return
  cache_dir = os.makedirs(path, exist_ok=True)
  return cache_dir

def pdfinfo(src):
  dat = dict()
  pipe = subprocess.Popen(['pdfinfo', src], stdout=subprocess.PIPE)
  for line in iter(pipe.stdout.readline, b''):
    k, v = map(lambda b: b.decode('utf8').strip(), line.split(b':', 1))
    if v.isnumeric():
      v = int(v)
    dat[k] = v
  return dat

def pdftoppm(src, dst, page):
  chunksize = 2**12
  args = ['pdftoppm', *PPLR_IMGTOPPM_ARGS, '-f', str(page), '-l', str(page), src]
  pipe = subprocess.Popen(args, stdout=subprocess.PIPE)
  with open(dst, 'wb') as fp:
    while True:
      b = pipe.stdout.read(chunksize)
      if not b: break
      fp.write(b)
  return

def ppmtopdf(src, dst):
  if dst.endswith('.pdf'):
    dst = dst[:-4]
  args = ['tesseract', src, dst, *TESS_IMGTOPDF_ARGS]
  subprocess.call(args)
  return

def pdftotxt(src, dst):
  args = ['pdftotext', *PPLR_PDFTOTXT_ARGS,  src, dst]
  subprocess.call(args, stdout=sys.stdout, stderr=sys.stderr)
  return

def append_txt(src, dst, nl=b'\n'):
  chunksize = 2**12
  with open(src, 'rb') as src_fp:
    with open(dst, 'ab') as dst_fp:
      dst_fp.write(nl)
      while True:
        b = src_fp.read(chunksize)
        if not b: break
        dst_fp.write(b)

def convert_to_text(pdf_path, target, cache=None, page_num_handler=lambda page_num: None):
  if not os.path.exists(target) and not os.path.isfile(target):
    open(target, 'x').close()
  _generate_cache_dir(cache)
  info = pdfinfo(pdf_path)
  page_count = info.get('Pages', 1)
  for page_num in range(1, page_count+1):
    page_num_handler(page_num)
    img_temp = os.path.join(cache_dir, f'{page_num}.ppm')
    pdf_temp = os.path.join(cache_dir, f'{page_num}.pdf')
    txt_temp = os.path.join(cache_dir, f'{page_num}.txt')
    pdftoppm(pdf_path, img_temp, page_num)
    ppmtopdf(img_temp, pdf_temp)
    os.unlink(img_temp)
    pdftotxt(pdf_temp, txt_temp)
    os.unlink(pdf_temp)
    append_txt(txt_temp, target)
    os.unlink(txt_temp)
  _clean_cache_dir()
