"""FEC Bulk Data Assets - Raw FEC file parsers using original FEC field names

CORE 6 FILES (focusing on raw transactional data) - ALL CONVERTED TO ARANGODB:
- cn.zip → cn collection (candidate master)
- cm.zip → cm collection (committee master)
- ccl.zip → ccl collection (candidate-committee linkages)
- pas2.zip → pas2 collection (itemized transactions - ALL types)
- oth.zip → oth collection (other receipts - PAC-to-PAC transfers)
- indiv.zip → indiv collection (individual contributions)

DEPRECATED (moved to src/assets/deprecated/):
- weball.zip → weball collection (candidate summary - derive from raw data)
- webl.zip → webl collection (committee summary - derive from raw data)
- webk.zip → webk collection (PAC summary - derive from raw data)
"""

from .cn import cn_asset
from .cm import cm_asset
from .ccl import ccl_asset
from .pas2 import pas2_asset
from .oth import oth_asset
from .indiv import indiv_asset

__all__ = [
    'cn_asset',
    'cm_asset',
    'ccl_asset',
    'pas2_asset',
    'oth_asset',
    'indiv_asset',
]
