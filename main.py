from maestro import Controller

if __name__ == "__main__":
    cnt = Controller("./test/test01.json")
    print(cnt._dag)
    print(cnt._queue)
    cnt.start(workers=3)

