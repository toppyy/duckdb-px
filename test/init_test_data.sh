#!bin/bash

wget https://pxweb2.stat.fi/database/StatFin/vaerak/statfin_vaerak_pxt_11rc.px          -O ./test/data/statfin_vaerak_pxt_11rc.px
wget https://pxweb2.stat.fi/database/StatFin_Passiivi/akay/statfinpas_akay_pxt_001.px   -O ./test/data/statfin_akay_pxt_001.px


curl -X POST https://api.scb.se/OV0104/v1/doris/en/ssd/START/BE/BE0101/BE0101A/BefolkningR1860N \
    -H "Content-Type: application/json" \
    -d @./test/data/query-BefolkningR1860N.json \
    -o "./test/data/SCB-BefolkningR1860N.px" 

