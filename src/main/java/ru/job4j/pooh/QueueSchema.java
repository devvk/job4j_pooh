package ru.job4j.pooh;

import java.util.concurrent.*;

public class QueueSchema implements Schema {
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    /**
     * Для каждой очереди отдельно берёт её сообщения
     * и раздаёт их подписчикам этой же очереди по кругу.
     */
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            for (var name : receivers.keySet()) {
                var queue = data.getOrDefault(name, new LinkedBlockingQueue<>());
                var receiversList = receivers.get(name);
                var iterator = receiversList.iterator();
                while (iterator.hasNext()) {
                    var data = queue.poll();
                    if (data != null) {
                        iterator.next().receive(data);
                    }
                    if (data == null) {
                        break;
                    }
                    if (!iterator.hasNext()) {
                        iterator = receiversList.iterator();
                    }
                }
            }
            condition.off();
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
