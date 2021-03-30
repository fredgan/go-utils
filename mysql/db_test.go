package mysql

import (
	"fmt"
	"testing"
	"time"

	"github.com/fredgan/go-utils/log"
)

const (
	_host    = "mysql"
	_port    = 3306
	_user    = "root"
	_pwd     = ""
	_db      = "test"
	_charset = "utf8"
)

func TestMaxConnect(t *testing.T) {

	connectWait, maxIdleConns, maxConns := time.Second*10, 1, 3

	conn, err := OpenEx(_host, _port, _user, _pwd, _db, maxIdleConns, maxConns, connectWait, _charset)
	if err != nil {
		t.Fatalf("Can noet connect(h=%s,P=%d,u=%s,p=%s,D=%s) err=%s",
			_host, _port, _user, _pwd, _db, err)
	}
	type Row struct {
		Id int `db:"id"`
	}
	for i := 0; i < maxConns+5; i++ {
		go func() {
			var rows = []*Row{}
			t.Log("select sleep(2s)")
			conn.Exec("set wait_timeout=2")
			if err := conn.Query(&rows, "SELECT SLEEP(2) as id"); err != nil {
				t.Fatalf("sleep(2) err=%s", err)
			} else {
				t.Log("select sleep(2) success.")
			}
		}()
	}
	time.Sleep(time.Second * 1)

	var test = func() {
		for i := 0; i < maxConns; i++ {
			var rows = []*Row{}
			log.Info("Start Query")
			if err := conn.Query(&rows, "SELECT 123 as id"); err != nil {
				t.Fatalf("Query Err=%s", err)
			}
			if len(rows) == 0 {
				t.Fatalf("Select nil ?")
			} else {
				log.Error("Success")
			}
		}
	}
	test()
	t.Log("sleep the wait connect timeout.")
	time.Sleep(time.Second * 10)
	test()
	t.Log("end test")
}

func TestQuery(t *testing.T) {
	conn, err := Open(_host, _port, _user, _pwd, _db, 128)
	l, _ := NewSqlLogger("./test.sql.log")
	conn.OpenSqlLog.Set(true)
	conn.SetLogger(l)
	if err != nil {
		t.Fatalf("Can noet connect(h=%s,P=%d,u=%s,p=%s,D=%s) err=%s",
			_host, _port, _user, _pwd, _db, err)
	}

	res, err := conn.Exec("CREATE TABLE IF NOT EXISTS `tb001`(id int PRIMARY KEY AUTO_INCREMENT, val char(32));")
	if err != nil {
		t.Fatalf("Can not create table[tb001] err=%s", err)
	}

	type Row struct {
		Id  int    `db:"id"`
		Val string `db:"val"`
	}

	INSERT := "INSERT INTO tb001(val) VALUES(\"I'm a Value.\")"
	SELECT := "SELECT * FROM `tb001`  ORDER BY id DESC LIMIT 1"
	SELECT_F := "SELECT * FROM `tb001` WHERE id =%d"
	UPDATE := "UPDATE `tb001` SET val='I update the value.' WHERE id=%d"

	res, err = conn.Exec(INSERT)
	if err != nil {
		t.Fatalf("Can not Insert err=%s", err)
	}

	id := res.LastInsertId
	if res, err = conn.Exec(fmt.Sprintf(UPDATE, id)); err != nil || res.RowsAffected != 1 {
		t.Fatalf("UPDATE err=%s Or res.RowsAffected is no One,=%d", err, res.RowsAffected)
	}

	var rows = []*Row{}
	if err := conn.Query(&rows, SELECT); err != nil {
		t.Fatalf("Query Err=%s", err)
	}

	if len(rows) == 0 {
		t.Fatalf("Select nil ?")
	}

	var rows2 = []*Row{}
	if err := conn.Query(&rows2, fmt.Sprintf(SELECT_F, id)); err != nil {
		t.Fatalf("Queryf Err=%s", err)
	}

	// test Query(Update)--------------
	var rows3 = []*Row{}
	err = conn.Query(&rows3, INSERT, id)
	if err == nil {
		t.Fatalf("WTF? Query a Update SQL?? ")
	}

	DELETE := "DELETE FROM tb001 WHERE id=%d"
	err = conn.Query(&rows3, fmt.Sprintf(DELETE, id))
	if err != nil {
		t.Fatalf("Delete err=%s Or res.RowsAffected is no One,=%d", err)
	}
	l.Close()
	time.Sleep(time.Second)
}
