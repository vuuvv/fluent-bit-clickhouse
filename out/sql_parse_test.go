package out

import (
	"encoding/json"
	"fmt"
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

type MyStruct struct {
	MyInt json.RawMessage `json:"myInt"`
}

func TestJsonParse(t *testing.T) {
	//jsonStr := `{"level":"info","ts":"2023-08-18T16:31:21.886+0800","caller":"server/server.go:245","_msg":"request","machine":"knode1","type":"req","status":200,"method":"GET","path":"/api/user/wx/login/qr/status","action":"系统管理::用户管理-获取登录二维码状态","forward":"/user/wx/login/qr/status","ip":"192.168.1.50","query":"token=54843238297661440&t=1692347478939","start":"2023-08-18T16:31:21.886+0800","latency":0.000892531,"trace":"54843336961294336"}`
	jsonStr := `{"level":"info","ts":"2023-08-21T10:00:11.871+0800","caller":"server/middlewares.go:203","_msg":"request","machine":"meter-app-579dcdf7df-ldm49","status":200,"method":"POST","type":"req","path":"/meter/device/current","action":"水电表管理::设备-当前值使用量","query":"","ip":"10.244.1.36","user-agent":"go-resty/2.7.0 (https://github.com/go-resty/resty)","latency":0.004982347,"form":{"mac":"","sn":"420000000022"}}`

	var reqLog ReqLog
	err := json.Unmarshal([]byte(jsonStr), &reqLog)
	if err != nil {
		fmt.Println("解析JSON出错:", err)
		return
	}

	fmt.Println("解析后的字符串:", reqLog.Status)
}
