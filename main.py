"""

  """

import asyncio
import json

import pandas as pd
from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq
from mirutil.tsetmc import search_tsetmc_async
from mirutil.utils import ret_clusters_indices


class GDUrl :
    with open('gdu.json' , 'r') as fi :
        gj = json.load(fi)

    cur = gj['cur']
    src = gj['src']
    trg = gj['trg']

gu = GDUrl()

class ColName :
    ftic = 'FirmTicker'
    sk = 'srchkey'
    tic = 'Ticker'
    name = "Name"
    cname = 'CompanyName'

c = ColName()

def main() :
    pass

    ##

    gds = GithubData(gu.src)
    gds.overwriting_clone()
    ##
    df = gds.read_data()
    ##
    da = pd.DataFrame()

    clus = ret_clusters_indices(df)

    for se in clus :
        si , ei = se
        print(se)

        nms = df.iloc[si :ei][c.ftic]

        _da = asyncio.run(search_tsetmc_async(nms))

        da = pd.concat([da , _da])

        # break

    ##
    msk = da[c.tic].eq(da[c.sk])
    db = da[msk]
    ##
    db = db[[c.tic , c.name]]
    ##
    db = db.drop_duplicates()
    ##
    ren = {
            c.tic  : c.ftic ,
            c.name : c.cname ,
            }
    db = db.rename(columns = ren)
    ##

    gdt = GithubData(gu.trg)
    gdt.overwriting_clone()
    ##
    dft = gdt.read_data()
    ##

    dft = pd.concat([dft , db])
    ##
    dft = dft.drop_duplicates()
    ##

    sprq(dft , gdt.data_fp)
    ##

    msg = "added to data by: "
    msg += gu.cur
    ##

    gdt.commit_and_push(msg)

    ##

    gdt.rmdir()
    gds.rmdir()


    ##

##
if __name__ == '__main__' :
    main()

##
