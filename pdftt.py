import os
import sys
import shutil
import subprocess
import tempfile
from multiprocessing.dummy import Pool as ThreadPool

PPLR_IMGTOPPM_ARGS = ['-r', '150', '-aa', 'yes', '-hide-annotations', '-gray', '-cropbox', '-q']
PPLR_PDFTOTXT_ARGS = ['-layout', '-nopgbrk', '-q']
TESS_IMGTOPDF_ARGS = ['-l', 'eng+ind+jav', '--psm', '6', 'pdf']

cache_dir = None
_num_digit_divisors = [10**n for n in range(64)]

def split_ext(s: str):
  if not '.' in s:
    return s, ''
  c = len(s) - 1
  while s[c] != '.':
    c -= 1
  return s[:c], s[c:]

def _num_digit(n: int):
  n = abs(n)
  if n == 0:
    return 1
  for i, v in enumerate(_num_digit_divisors):
    if n - v < 0:
      return i
  return len(f'{n}')

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
    return cache_dir
  os.makedirs(path, exist_ok=True)
  cache_dir = path
  return cache_dir

def pdfinfo(src):
  dat = dict()
  pipe = subprocess.Popen(['pdfinfo', src], stdout=subprocess.PIPE, stderr=sys.stderr)
  for line in iter(pipe.stdout.readline, b''):
    k, v = map(lambda b: b.decode('utf8').strip(), line.split(b':', 1))
    if v.isnumeric():
      v = int(v)
    dat[k] = v
  return dat

def pdftoppm(src, dst, page):
  chunksize = 2**12
  args = ['pdftoppm', *PPLR_IMGTOPPM_ARGS, '-f', str(page), '-l', str(page), src]
  pipe = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=sys.stderr)
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
  subprocess.call(args, stdout=sys.stdout, stderr=sys.stderr)
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
  return

def pdf_to_text(source, target, cache=None, num_threads=2, min_page=1, max_page=None):
  if os.path.exists(target) and os.path.isfile(target):
    os.unlink(target)
  open(target, 'x').close()

  _generate_cache_dir(cache)

  info = pdfinfo(source)
  page_count = info.get('Pages', 1)
  if max_page and max_page < page_count:
    page_count = max_page
  page_digit = _num_digit(page_count)
  fileno_fmt = '{:0%dd}' % page_digit

  def job(page_num):
    fileno = fileno_fmt.format(page_num)
    img_temp = os.path.join(cache_dir, fileno + '.ppm')
    pdf_temp = os.path.join(cache_dir, fileno + '.pdf')
    txt_temp = os.path.join(cache_dir, fileno + '.txt')
    pdftoppm(source, img_temp, page_num)
    ppmtopdf(img_temp, pdf_temp)
    os.unlink(img_temp)
    pdftotxt(pdf_temp, txt_temp)
    os.unlink(pdf_temp)
    return txt_temp

  pool = ThreadPool(num_threads)
  txt_paths = pool.map(job, range(min_page, page_count+1))
  pool.close()
  pool.join()

  for n, txt_path in enumerate(sorted(txt_paths)):
    append_txt(txt_path, target, nl=b'='*82+b'\n')
    os.unlink(txt_path)

  _clean_cache_dir()
  return

if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser(prog='pdftt',
                                   description='converts pdf to txt, requires poppler and tesseract-ocr')
  parser.add_argument('pdf')
  parser.add_argument('-o', '--outfile', type=str)
  parser.add_argument('-f', '--minpage', type=int, default=1)
  parser.add_argument('-l', '--maxpage', type=int, default=None)
  parser.add_argument('-t', '--threads', type=int, default=2)
  parser.add_argument('--cache-dir', type=str, default=None)
  arguments = parser.parse_args()
  pdf_to_text(arguments.pdf,
              arguments.outfile if arguments.outfile else split_ext(arguments.pdf)[0] + '.txt',
              arguments.cache_dir,
              arguments.threads,
              arguments.minpage,
              arguments.maxpage
              )
