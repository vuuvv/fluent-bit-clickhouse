package main

import (
	"testing"
)

func TestParseSqlInfo(t *testing.T) {
	log := SqlLog{}
	//input := "/app/services/logger.go:77 SLOW SQL >= 200ms\n[11049.392ms] [rows:5] select level from logs group by level"
	input := "\n/app/controllers/meter.go:282\n[0.566ms] [rows:0] SELECT * FROM \"t_meter\" WHERE mac = '863100062369495' AND \"t_meter\".\"trashed\" = 0"
	input = "/app/services/equipment_order.go:245 SLOW SQL >= 200ms\n[402.179ms] [rows:641] select * from t_equipment_order  where estate_id=38923563229544448 and created_at>='2023-08-17 00:00:00' and created_at<'2023-08-17 23:59:59' ORDER BY created_at  limit 1000 OFFSET 6000"
	input = "/app/services/org.go:48 SLOW SQL >= 200ms\n[330.528ms] [rows:20] select o.*, p.name as parent_name, ot.title as type_name\n\nfrom t_org as o\njoin t_org_type as ot on ot.id=o.type_id\nleft join t_org as p on p.id=o.parent_id\nwhere \"o\".\"path\" LIKE 'BAAA%' and 1=1 and \"ot\".\"value\" = 'estate' and 1=1 and 1=1 and  o.deleted_at=0\nORDER BY \"o\".\"id\" DESC\nLIMIT 20 offset 0"
	ParseSqlLog(input, &log)
	t.Log(log.Sql)
	t.Log(log.File)
	t.Log(log.Ms)
	t.Log(log.Rows)
}
