from locust import Locust, HttpLocust, TaskSet, task
import json
import random
import resource
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))
setUsers = set()
setOrders = set()
setItems = set()

class Webshop(TaskSet):
    @task(1)
    def orderCheckout(self):
        if (len(setOrders) > 0) & (len(setItems) > 0):
            order_id = random.choice(list(setOrders))
            item_id = random.choice(list(setItems))
            setOrders.remove(order_id)

            plus_amount = 10
            ## Make sure there is something to checkout
            response = self.client.post("/orders/addItem/"+order_id+"/" + item_id, name="/orders/addItem/{order_id}/{item_id}")
            response = self.client.post("/stock/add/" + item_id + "/" +str(plus_amount), name="/stock/add/{item_id}/{number}")

            response = self.client.post("/orders/checkout/" + order_id, name="/orders/checkout/{order_id}")
            data = json.loads(response.text)
            print(data)
            assert data['result']['result'] == 'success'

class WebsiteUser(HttpLocust):
    task_set = Webshop
    min_wait = 1000
    max_wait = 9000
