import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HandlerImpl implements Handler {
    private final Client client;
    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 10);//TODO find optimal threads pool number

    public HandlerImpl(Client client) {
        this.client = client;
    }

    @Override
    public Duration timeout() {
        return Duration.ofSeconds(3); // Example timeout, adjust as needed
    }

    @Override
    public void performOperation() {
        //TODO add a mechanism to gracefully exit this loop as needed
        while (true) {
            Event event = client.readData();
            processEvent(event);
        }
    }

    private void processEvent(Event event) {
        List<Address> recipients = event.recipients();
        Payload payload = event.payload();

        // Initiate send operations without blocking to wait for them to complete.
        recipients.forEach(address ->
                CompletableFuture.runAsync(() -> sendWithRetry(address, payload), executorService)
        );
    }

    private void sendWithRetry(Address dest, Payload payload) {
        boolean sent = false;
        while (!sent) {
            Result result = client.sendData(dest, payload);
            if (result == Result.ACCEPTED) {
                sent = true;
            } else if (result == Result.REJECTED) {
                try {
                    Thread.sleep(timeout().toMillis()); // Delay before retry, consider using a non-blocking delay
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry delay", e);
                }
            }
        }
    }
}
