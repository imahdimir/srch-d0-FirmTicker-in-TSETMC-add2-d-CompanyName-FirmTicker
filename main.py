##

"""

  """

##


import pandas as pd
from pathlib import Path

from githubdata import GithubData
from mirutil import funcs as mf
from mirutil.funcs import save_df_as_a_nice_xl as sxl


btic_repo_url = 'https://github.com/imahdimir/d-uniq-BaseTickers'
tseid_repo_url = 'https://github.com/imahdimir/d-uniq-TSETMC_ID'

tic2btic_url = 'https://github.com/imahdimir/d-Ticker-2-BaseTicker-map'

burl = f'http://www.tsetmc.com/Loader.aspx?ParTree=151311&i='

btic = 'BaseTicker'
tick = 'Ticker'
ticn = 'TickerN'
name = 'Name'
tid = 'TSETMC_ID'
title = 'Title'
mkt = 'Market'

idcols = {
    'ID-1' : '' ,
    'ID-2' : 2 ,
    'ID-3' : 3 ,
    'ID-4' : 4 ,
    }

def main() :
  pass

  ##

  btics = GithubData(btic_repo_url)
  btics.clone()
  ##
  bdfpn = btics.data_filepath
  bdf = pd.read_excel(bdfpn)

  bdf = bdf.reset_index()
  bdf = bdf[[btic]]
  ##
  sdf = pd.DataFrame()
  ##

  for _ , row in bdf.iterrows() :
    _sdf = mf.search_tsetmc(row[btic])
    sdf = pd.concat([sdf , _sdf])

  ##
  if Path('temp.prq').exists() :
    _df = pd.read_parquet('temp.prq')
    sdf = pd.concat([sdf , _df])

  ##
  sdf[tick] = sdf[tick].str.strip()
  sdf[name] = sdf[name].str.strip()
  ##
  sdf = sdf.drop_duplicates()
  ##
  msk = sdf[tick].isna()
  for cn in sdf.columns :
    msk |= sdf[cn].isna()

  df1 = sdf[msk]
  ##
  sdf = sdf.dropna()
  sdf.to_parquet('temp.prq' , index = False)
  ##
  idf = pd.DataFrame()
  for ky , vl in idcols.items() :
    _idf = sdf[sdf.columns.difference(set(idcols.keys()) - set(ky))]

    _idf[tid] = sdf[[ky]]
    _idf[ticn] = sdf[tick] + str(vl)

    idf = pd.concat([idf , _idf])

  assert idf[tid].is_unique
  ##
  tid_repo = GithubData(tseid_repo_url)
  tid_repo.clone()
  ##
  tidfpn = tid_repo.data_filepath
  tidf = pd.read_excel(tidfpn)
  ##
  tidf = pd.concat([tidf , idf[[tid]]])
  ##
  tidf = tidf.drop_duplicates()
  ##
  sxl(tidf , tidfpn)
  ##
  msg = 'init - added by searching BaseTickers'
  tid_repo.commit_push(msg)

  ##
  mf.save_as_prq_wo_index(idf , 'temp1.prq')
  ##
  idf = pd.read_parquet('temp1.prq')

  ##
  msk = idf.duplicated(subset = [ticn , 'Market' , name , 'IsActive'] ,
                       keep = False)
  df1 = idf[msk]
  ##
  msk = idf.duplicated(subset = idf.columns.difference([tid]) , keep = False)
  df1 = idf[msk]
  ##

  btics.rmdir()
  tid_repo.rmdir()

  ##

##