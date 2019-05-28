package kaflinkshop;

import kaflinkshop.Order.OrderJob;
import kaflinkshop.Payment.PaymentJob;
import kaflinkshop.Stock.StockJob;
import kaflinkshop.User.UserJob;

import java.util.Arrays;
import java.util.stream.Stream;

public class RunAll {

	public static void main(String[] args) throws Exception {

		Program[] programs = {
				UserJob::main,
				StockJob::main,
				PaymentJob::main,
				OrderJob::main,
		};

		Stream<Thread> threads = Arrays.stream(programs).map(Thread::new);

		threads.forEach(Thread::run);

		threads.forEach(t -> {
			try {
				t.join();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}

	private static interface Program extends Runnable {

		public void main(String[] args) throws Exception;

		default void run() {
			try {
				main(new String[0]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
