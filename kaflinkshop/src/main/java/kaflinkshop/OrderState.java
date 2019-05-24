package kaflinkshop;

import java.util.HashMap;

public class OrderState {
    public String order_id;
    public String user_id;
    public HashMap<String, Integer> products;
    public long last_modified;
    public boolean check_user;
    public boolean is_paid;
}
