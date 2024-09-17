package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"go-web-app/models"
	"go-web-app/pkg/snowflake"
	"go-web-app/pkg/todaytime"
	"go-web-app/settings"
	clientv3 "go.etcd.io/etcd/client/v3"
	"io/ioutil"
	"strconv"
	"time"
)

const (
	JobDir  = "/cron/jobs/"
	JobKill = "/cron/kill/"
)

var (
	clinet  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	GJobmgr *models.JobMgr
	oldjob  *models.Job
)

type JobMgr struct {
	Kv     clientv3.KV
	Lease  clientv3.Lease
	Clinet *clientv3.Client
}

func InitCrontab(cfg *settings.EtcdConfig) (err error) {
	// 加载 CA 证书
	caCert, err := ioutil.ReadFile(cfg.CaCert)
	if err != nil {
		fmt.Println("加载 CA 证书失败：", err)
		return
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		fmt.Println("解析 CA 证书失败")
		return
	}

	// 加载客户端证书和私钥
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		fmt.Println("加载客户端证书和私钥失败：", err)
		return
	}

	// 创建 TLS 配置
	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,              // 信任的 CA
		Certificates: []tls.Certificate{cert}, // 客户端证书
		ServerName:   cfg.ServerName,          // etcd 服务器的域名
	}

	// 配置 etcd 客户端
	config := clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: time.Duration(cfg.DialTimeout) * time.Millisecond,
		TLS:         tlsConfig,
		Username:    cfg.Username,
		Password:    cfg.Password,
	}

	// 创建 etcd 客户端
	if clinet, err = clientv3.New(config); err != nil {
		fmt.Println("连接 etcd 失败：", err)
		return
	}

	// 获取 KV 和 Lease 的 API 子集
	kv = clientv3.NewKV(clinet)
	lease = clientv3.NewLease(clinet)

	// 赋值单例
	GJobmgr = &models.JobMgr{
		Clinet: clinet,
		Kv:     kv,
		Lease:  lease,
	}

	return
}

func SaveJob(jobmgr *models.JobMgr, job models.CrontabJob) (oldJob *models.Job, err error) {

	var (
		jobKey   string
		jobValue []byte
		putResp  *clientv3.PutResponse
	)
	jobetcd := models.Job{
		Name:     job.JobName,
		Command:  job.JobShell,
		CronExpr: job.JobCronExpr,
	}
	jobKey = JobDir + job.JobName
	if jobValue, err = json.Marshal(jobetcd); err != nil {
		return
	}
	if putResp, err = jobmgr.Kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJob); err != nil {
			err = nil
			return
		}

	}
	return
}
func DeleteJob(jobmgr *models.JobMgr, job models.Job) (oldJob *models.Job, err error) {

	var (
		jobKey  string
		DelResp *clientv3.DeleteResponse
	)
	jobKey = JobDir + job.Name
	if DelResp, err = jobmgr.Kv.Delete(context.TODO(), jobKey); err != nil {
		return
	}
	if len(DelResp.PrevKvs) != 0 {
		if err = json.Unmarshal(DelResp.PrevKvs[0].Value, &oldjob); err != nil {
			err = nil
			return
		}
	}
	return
}
func KillJob(client *models.ParameCrontab) (r int, err error) {
	var (
		killKey        string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseid        clientv3.LeaseID
	)
	killKey = JobKill + client.JobName

	if leaseGrantResp, err = GJobmgr.Lease.Grant(context.TODO(), 1); err != nil {
		fmt.Println(err)
		return
	}
	leaseid = leaseGrantResp.ID

	if _, err = GJobmgr.Kv.Put(context.TODO(), killKey, "", clientv3.WithLease(leaseid)); err != nil {
		return
	}
	r = 1
	return
}
func CheckJob(client *models.ParameCrontab) (err error) {
	sqlStr := `select count(jobname)  from joblist where jobname=?`
	var count int
	if err := db.Get(&count, sqlStr, client.JobName); err != nil {
		return err
	}
	if count > 0 {
		return ErrorHostExist
	}

	return
}
func CrontabAdd(client *models.ParameCrontab) (Reply int64, err error) {
	if oldjob, err = SaveJob(GJobmgr, client.CrontabJob); err != nil {
		fmt.Println(err)
	}
	sqlStr := "insert into joblist(jobid,jobname,jobshell,jobstarttime,jobstatus,jobcronexpr) values (?,?,?,?,?,?)"
	ret, err := db.Exec(sqlStr,
		snowflake.IdNum(),
		client.JobName,
		client.JobShell,
		todaytime.NowTimeFull(),
		client.JobStatus,
		client.JobCronExpr,
	)
	if err != nil {
		fmt.Println(err)
		if oldjob, err = DeleteJob(GJobmgr, client.Job); err != nil {
			fmt.Println(err)
		}
		return
	}
	clinet := models.SystemLog{
		SystemlogHostName:  "所有主机",
		SystemlogType:      "新增定时任务",
		SystemlogInfo:      sqlStr,
		SystemlogStartTime: todaytime.NowTimeFull(),
	}
	if err == nil {
		clinet.SystemlogNote = "成功"
	} else {
		clinet.SystemlogNote = err.Error()
	}
	_, err = SystemLogInsert(clinet)
	Reply, err = ret.RowsAffected()
	if err != nil {
		fmt.Println(err)
		return
	} else {
		fmt.Printf("更新数据为 %d 条\n", Reply)
	}
	return

}
func CrontabDel(client *models.ParameCrontab) (Reply int64, err error) {
	if oldjob, err = DeleteJob(GJobmgr, client.Job); err != nil {
		fmt.Println(err)
	}
	sqlStr := "delete  from joblist where jobid=?"
	ret, err := db.Exec(sqlStr, client.JobId)
	if err != nil {
		return
	}
	Reply, err = ret.RowsAffected()
	if err != nil {
		return
	} else {
		fmt.Printf("删除数据 %d 条\n", Reply)
	}
	clinet := models.SystemLog{
		SystemlogHostName:  "所有主机",
		SystemlogType:      "删除定时任务",
		SystemlogInfo:      sqlStr,
		SystemlogStartTime: todaytime.NowTimeFull(),
	}
	if err == nil {
		clinet.SystemlogNote = "成功"
	} else {
		clinet.SystemlogNote = err.Error()
	}
	_, err = SystemLogInsert(clinet)
	return

}
func CrontabEdit(client *models.ParameCrontab) (Reply int64, err error) {
	if oldjob, err = SaveJob(GJobmgr, client.CrontabJob); err != nil {
		fmt.Println(err)
	}
	sqlStr := `update joblist set jobstatus=?,jobshell=?,jobname=?,jobcronexpr=? where jobid=? `
	ret, err := db.Exec(sqlStr,
		client.JobStatus,
		client.JobShell,
		client.JobName,
		client.JobCronExpr,
		client.JobId,
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	Reply, err = ret.RowsAffected()
	if err != nil {
		fmt.Println(err)
		return
	} else {
		fmt.Printf("更新数据为 %d 条\n", Reply)
	}
	clinet := models.SystemLog{
		SystemlogHostName:  "所有主机",
		SystemlogType:      "修改定时任务",
		SystemlogInfo:      sqlStr,
		SystemlogStartTime: todaytime.NowTimeFull(),
	}
	if err == nil {
		clinet.SystemlogNote = "成功"
	} else {
		clinet.SystemlogNote = err.Error()
	}
	_, err = SystemLogInsert(clinet)
	return

}
func CrontabSelect(client *models.ParameCrontab) (Reply []models.CrontabJob, err error) {
	sqlStr := "select jobid,jobname,jobshell,jobstarttime,jobstatus,jobcronexpr from joblist "
	if err := db.Select(&Reply, sqlStr); err != nil {
		return Reply, err
	}
	return

}
func CrontabTotal(client *models.ParameCrontab) (total int, err error) {
	sqlStr := `select count(jobid)  from joblist`
	if err := db.Get(&total, sqlStr); err != nil {
		return total, err
	}
	return
}
func CrontabOnline(client *models.ParameCrontab) (total int, err error) {
	client.JobStatus = 1
	sqlStr := `select count(jobid)  from joblist where jobstatus= ?`
	if err := db.Get(&total, sqlStr, client.JobStatus); err != nil {
		return total, err
	}
	return
}
func CrontabTodayTotal(client *models.ParameCrontab) (total int, err error) {
	now := time.Now()

	sqlStr := `select count(jobid)  from joblist where jobstarttime > ? `
	if err := db.Get(&total, sqlStr, now.Format("2006-01-02")+" 00:00:00"); err != nil {
		return total, err
	}
	return
}
func CrontabAddToday(client *models.ParameCrontab) (total int, err error) {
	now := time.Now()
	client.JobStatus = 1
	sqlStr := `select count(jobid)  from joblist where jobstatus= ? and jobstarttime > ? `
	if err := db.Get(&total, sqlStr, client.JobStatus, now.Format("2006-01-02")+" 00:00:00"); err != nil {
		return total, err
	}
	return
}

func TaskJobLog(client *models.ParameCrontab) (Reply int64, err error) {
	starttime1, _ := strconv.ParseInt(client.JobStartTime, 10, 64)
	starttime2 := time.Unix((starttime1+28800000)/1000, 0)
	stoptime1, _ := strconv.ParseInt(client.JobStopTime, 10, 64)
	stoptime2 := time.Unix((stoptime1+28800000)/1000, 0)
	jobrunning := stoptime1 - starttime1
	jobrunning1 := jobrunning / 1000
	sqlStr := "insert into jobdata(jobname,jobstarttime,jobstoptime,jobinfo,jobrunning,joberr) values (?,?,?,?,?,?)"
	ret, err := db.Exec(sqlStr,
		client.JobName,
		starttime2,
		stoptime2,
		client.JobInfo,
		jobrunning1,
		client.JobErr,
	)
	Reply, err = ret.RowsAffected()
	if err != nil {
		fmt.Println(err)
		return
	} else {
		fmt.Printf("更新数据为 %d 条\n", Reply)
	}
	return

}

func TaskJobLogSelect(client *models.ParameCrontab) (Reply []models.CrontabJob, err error) {

	sqlStr := "select jobinfo,jobstarttime,jobstoptime,jobrunning from jobdata where jobname=? ORDER BY `jobstoptime` DESC LIMIT 0,10 "
	if err := db.Select(&Reply, sqlStr, client.JobName); err != nil {
		return Reply, err
	}
	return
}

func LogmsgGet(client *models.ParameCrontab) (hostgetdata []models.Alarmlist, err error) {
	sqlStr := ` select a.hostid as alarmid,
        a.alarmtype,
        hostlist.hostowner as alarmhostonwer,
        hostlist.hostname as alarmhostname,
        hostlist.hostip as alarmhostip,
        a.alarminfo,
        a.alarmstarttime  
 from alarmstatistics as a join 
     hostlist on a.hostid=hostlist.hostid;`
	if err := db.Select(&hostgetdata, sqlStr); err != nil {
		return hostgetdata, err
	}
	return
}
func SystemLogInsert(client models.SystemLog) (Reply int64, err error) {
	sqlStr := "insert into systemlog(systemlogid,systemloghostname,systemlogtype,systemloginfo,systemlognote,systemlogstarttime) values (?,?,?,?,?,?)"
	ret, err := db.Exec(sqlStr,
		snowflake.IdNum(),
		client.SystemlogHostName,
		client.SystemlogType,
		client.SystemlogInfo,
		client.SystemlogNote,
		todaytime.NowTimeFull(),
	)
	Reply, err = ret.RowsAffected()
	if err != nil {
		fmt.Println(err)
		return
	} else {
		fmt.Printf("更新数据为 %d 条\n", Reply)
	}

	return
}

func SystemLogGet(client *models.ParameCrontab) (hostgetdata []models.SystemLog, err error) {
	sqlStr := " select systemlogid,systemlogtype,systemloginfo,systemloghostname,systemlognote,systemlogstarttime  from systemlog ORDER BY `systemlogstarttime` DESC LIMIT 0,100 "
	if err := db.Select(&hostgetdata, sqlStr); err != nil {
		return hostgetdata, err
	}
	return
}
