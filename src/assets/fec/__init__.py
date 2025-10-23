"""FEC Bulk Data Assets - Raw FEC file parsers using original FEC field names

Collection names match FEC file prefixes:
- cn.zip → cn collection (candidate master)
- cm.zip → cm collection (committee master)
- ccl.zip → ccl collection (candidate-committee linkages)
- weball.zip → weball collection (candidate summary - all)
- webl.zip → webl collection (committee summary)
- webk.zip → webk collection (PAC summary)
- pas2.zip → itpas2 collection (itemized transactions - ALL types including 24A/24E for independent expenditures)
- oppexp.zip → oppexp collection (operating expenditures)
"""

from .cn import cn_asset
from .cm import cm_asset
from .ccl import ccl_asset
from .weball import weball_asset
from .webl import webl_asset
from .webk import webk_asset
from .itpas2 import itpas2_asset
from .oppexp import oppexp_asset

__all__ = [
    'cn_asset',
    'cm_asset',
    'ccl_asset',
    'weball_asset',
    'webl_asset',
    'webk_asset',
    'itpas2_asset',
    'oppexp_asset',
]
