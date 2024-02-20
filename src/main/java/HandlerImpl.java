import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class HandlerImpl implements Handler {
    private final Client client;

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
            Event event = client.readData(); // Blocking call to read data
            List<Address> recipients = event.recipients();
            Payload payload = event.payload();

            CompletableFuture<?>[] futures = recipients.stream()
                    .map(address -> CompletableFuture.runAsync(() -> sendWithRetry(address, payload)))
                    .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(futures).join(); // Wait for all sending operations to complete
        }
    }

    private void sendWithRetry(Address dest, Payload payload) {
        boolean accepted = false;
        do {
            Result result = client.sendData(dest, payload);
            accepted = result == Result.ACCEPTED;
            if (!accepted) {
                try {
                    TimeUnit.MILLISECONDS.sleep(timeout().toMillis()); // Wait for the specified timeout before retrying
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Thread interrupted during sleep", e);
                }
            }
        } while (!accepted);
    }
}
