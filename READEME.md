# sqlwrapper


## Install

`go get github.com/syhlion/sqlwrapper`

## Usage

```
func main(){
    db, err := sql.Open("xxx","xxx")
    if err != nil {
        return nil, err
    }
    db := WrapperDB(db,true)


    // it log [sql] select * from member where id = ?  1  2s
    rs,err:=db.Exec("select * from member where id = ?",1)
    if err != nil {
        return
    }

}
```
