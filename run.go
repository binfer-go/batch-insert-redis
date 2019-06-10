package main

import (
	"crypto/rand"
	"fmt"
	"github.com/go-redis/redis"
	"runtime"
	"strconv"
	"time"
)

var (
	jobnum = runtime.NumCPU()
	//每次写入redis的数量
	//除以 jobnum 为了保证改变了任务数, 总量不变, 便于测试
	procnum = 5000 / jobnum
)

type Job struct {
	ID     string
	Client *redis.Client
	Result chan<- string
}

func main() {


	start := time.Now()
	fmt.Println("start:", start)
	defer func() {
		end := time.Now()
		fmt.Println("end:", end)
		fmt.Println("jobs num:", jobnum, "total items:", jobnum*procnum)
		fmt.Println("total seconds:", end.Sub(start).Seconds())
	}()

	//任务channel 定义缓冲器为job数量
	jobs := make(chan Job, jobnum)

	//存放结果
	results := make(chan string, jobnum*procnum)

	//每个任务完成之后给dones发送一次
	dones := make(chan struct{}, jobnum)
	client := initClient(100)
	defer client.Close()

	//定义每个任务执行的方法
	jobfunc := func(client *redis.Client, id string) (string, error) {
		defer func() {
			//完成之后向 dones 发送数据
			dones <- struct{}{}
			fmt.Println("job id:", id, "完成")
		}()

		//写入 procnum 条数据
		for idx := 0; idx < procnum; idx++ {
			key := "test:" + id + ":" + getUUID()

			//val, err :=
			_, err :=
				client.Set(key, time.Now().String(), 0).Result()
			if err != nil {
				return "", err
			}

		}

		return "ok", nil
	}

	//1 添加 job 到 channel
	go func() {
		for index := 0; index < jobnum; index++ {
			jobs <- Job{strconv.Itoa(index), client, results}
		}
		defer close(jobs)
	}()

	//2 并行执行 jobs
	for j := range jobs {
		go func(job Job) {
			jobfunc(client, job.ID)
			job.Result <- "ok"
		}(j)
	}

	//3 等待所有任务完成
	waitJobs(dones, results)
}



func waitJobs(dones <-chan struct{}, results chan string) {
	working := jobnum
	done := false
	for {
		select {
		case result := <-results:
			println(result)
		case <-dones:
			working--
			if working <= 0 {
				done = true
			}
		default:
			if done {
				return
			}
		}
	}
}

func initClient(poolSize int) *redis.Client {

	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     poolSize,
		//Password:     "womai@123",
		DB:           0,
	})
	if err := client.FlushDb().Err(); err != nil {
		panic(err)
	}
	return client
}

func getUUID() (uuid string) {

	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	uuid = fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])

	return
}



