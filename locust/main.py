from locust import Locust, HttpLocust, TaskSet, task
import json
import random
import resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
setUsers = set()
setOrders = set()
setItems = set()

class Webshop(TaskSet):
    ### User Tasks
    @task(1)
    def index(self):
        self.client.get("/")

    @task(2)
    def createUser(self):
        response = self.client.post("/users/create")
        data = json.loads(response.text)
        user_id = data['result']['params']['user_id']
        setUsers.add(user_id)

    @task(1)
    def removeUser(self):
        if len(setUsers) > 0:
            user_id = random.choice(list(setUsers))
            setUsers.remove(user_id)
            response = self.client.delete("/users/remove/" + user_id, name="/users/remove/[id]")

    @task(5)
    def findUser(self):
        if len(setUsers) > 0:
            user_id = random.choice(list(setUsers))
            response = self.client.get("/users/find/" + user_id, name="/users/find/[id]")

    @task(1)
    def creditRoutine(self):
        if len(setUsers) > 0:
            user_id = random.choice(list(setUsers))
            response = self.client.get("/users/credit/" + user_id, name="/users/credit/[id]")
            data = json.loads(response.text)
            credits = int(data['result']['params']['balance'])
            plus_amount = 20
            min_amount = 15
            response = self.client.post("/users/credit/add/" + user_id + "/" +str(plus_amount), name="/users/credit/add/[u_id]/[amount]")
            response = self.client.post("/users/credit/subtract/" + user_id + "/" +str(min_amount), name="/users/credit/subtract/[u_id]/[amount]")

            data = json.loads(response.text)
            new_credits = int(data['result']['params']['balance'])
            assert (new_credits - credits) == (plus_amount - min_amount)

    ### Stock tasks
    @task(10)
    def createItem(self):
        response = self.client.post("/stock/item/create", name="/stock/item/create")
        data = json.loads(response.text)
        item_id = data['result']['params']['item_id']
        setItems.add(item_id)

    @task(5)
    def itemRoutine(self):
        if len(setItems) > 0:
            item_id = random.choice(list(setItems))
            response = self.client.get("/stock/availability/" + item_id, name="/stock/availability/{item_id}")
            data = json.loads(response.text)
            credits = int(data['result']['params']['amount'])
            plus_amount = 20
            min_amount = 12
            response = self.client.post("/stock/add/" + item_id + "/" +str(plus_amount), name="/stock/add/{item_id}/{number}")
            response = self.client.post("/stock/subtract/" + item_id + "/" +str(min_amount), name="/stock/subtract/{item_id}/{number}")

            data = json.loads(response.text)
            new_credits = int(data['result']['params']['amount'])
            assert (new_credits - credits) == (plus_amount - min_amount)


    ### Order Tasks
    @task(4)
    def createOrder(self):
        if len(setUsers) > 0:
            user_id = random.choice(list(setUsers))
            response = self.client.post("/orders/create/" + user_id, name="/orders/create/{user_id}")
            data = json.loads(response.text)
            # print("Order id: " + data['result']['params']['order_id'])
            order_id = data['result']['params']['order_id']
            setOrders.add(order_id)

    @task(4)
    def removeOrderRoutine(self):
        if len(setOrders) > 0:
            order_id = random.choice(list(setOrders))
            setOrders.remove(order_id)

            ## Get the user id
            response = self.client.get("/orders/find/" + order_id, name="/orders/find/{order_id}")
            data = json.loads(response.text)
            user_id = data['result']['params']['user_id']

            ## Remove the order
            response = self.client.delete("/orders/remove/" + order_id, name="/orders/remove/{order_id}")

            ## Check if its gone
            response = self.client.get("/users/find/" + user_id, name="/users/find/[id]")
            data = json.loads(response.text)
            user_orders = data['result']['params']['orders']

            assert order_id not in user_orders

    @task(2)
    def addItemToOrderRoutine(self):
        if (len(setOrders) > 0) and (len(setItems) > 0):
            order_id = random.choice(list(setOrders))
            item_id = random.choice(list(setItems))
            response = self.client.post("/orders/addItem/"+order_id+"/" + item_id, name="/orders/addItem/{order_id}/{item_id}")
            data = json.loads(response.text)

            assert item_id in data['result']['params']['products']

    @task(2)
    def addItemToOrderRoutine(self):
        if (len(setOrders) > 0) and (len(setItems) > 0):
            order_id = random.choice(list(setOrders))
            item_id = random.choice(list(setItems))
            response = self.client.post("/orders/addItem/"+order_id+"/" + item_id, name="/orders/addItem/{order_id}/{item_id}")
            data = json.loads(response.text)

            assert item_id in data['result']['params']['products']

    @task(2)
    def removeItemFromOrderRoutine(self):
        if (len(setOrders) > 0):
            order_id = random.choice(list(setOrders))
            response = self.client.get("/orders/find/" + order_id, name="/orders/find/{order_id}")
            data = json.loads(response.text)

            items = list(data['result']['params']['products'])

            if len(items) > 0:
                item_id = random.choice(items)
                print(item_id)
                response = self.client.delete("/orders/removeItem/"+order_id+"/" + item_id, name="/orders/removeItem/{order_id}/{item_id}")


                response = self.client.get("/orders/find/" + order_id, name="/orders/find/{order_id}")
                data = json.loads(response.text)
                assert item_id not in data['result']['params']['products']

    # Not implemented yet
    # @task(1)
    def orderCheckout(self):
        if (len(setOrders) > 0):
            order_id = random.choice(list(setOrders))
            setOrders.remove(order_id)
            response = self.client.post("/orders/checkout/" + order_id, name="/orders/checkout/{order_id}")
            data = json.loads(response.text)
            assert data['result']['result'] == 'success'



class WebsiteUser(HttpLocust):
    task_set = Webshop
    min_wait = 1000
    max_wait = 9000
