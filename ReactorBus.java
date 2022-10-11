import lombok.AllArgsConstructor;
import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.*;
import java.util.stream.Collectors;

public class ReactorBus {

    private final Map<Class<?>, Sinks.Many<Event>> eventSinks = new HashMap<>();
    private final ThreadLocal<Queue<EventDispatch>> queue = ThreadLocal.withInitial(LinkedList::new);
    private final ThreadLocal<Boolean> dispatching = ThreadLocal.withInitial(() -> false);

    public void publish(Event event) {
        var sinks = getSinks(event.getClass());
        if (sinks.size() > 0) {
            var queue = this.queue.get();
            queue.offer(new EventDispatch(event, sinks));
            if (!dispatching.get()) {
                dispatching.set(true);
                EventDispatch eventDispatch = queue.poll();
                while (eventDispatch != null) {
                    eventDispatch.dispatch();
                    eventDispatch = queue.poll();
                }
                dispatching.set(false);
            }
        }
    }

    public <T extends Event> Flux<T> forEventType(Class<T> eventClass) {
        return (Flux<T>) eventSinks
                .computeIfAbsent(eventClass, k -> Sinks.many().multicast().onBackpressureBuffer(1000))
                .asFlux();

    }

    private List<Sinks.Many<Event>> getSinks(Class<?> eventType) {
        return getAllSuperClassesAndInterfaces(eventType).stream()
                .map(eventSinks::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private Set<Class<?>> getAllSuperClassesAndInterfaces(Class<?> clazz) {
        return getAllSuperClassesAndInterfaces(clazz, new LinkedHashSet<>());
    }

    private Set<Class<?>> getAllSuperClassesAndInterfaces(Class<?> clazz, Set<Class<?>> classSet) {
        while (clazz != null) {
            classSet.add(clazz);
            var interfaces = clazz.getInterfaces();
            Collections.addAll(classSet, interfaces);
            for (var directInterface : interfaces) {
                getAllSuperClassesAndInterfaces(directInterface, classSet);
            }
            clazz = clazz.getSuperclass();
        }
        return classSet;
    }

    @AllArgsConstructor
    private static class EventDispatch {

        private final Event event;
        private final List<Sinks.Many<Event>> sinks;

        public void dispatch() {
            sinks.forEach(sink -> sink.tryEmitNext(event));
        }
    }

    public static void main(String[] args) {
        List<String> a = List.of("1","2","3");

        ReactorBus reactorBus = new ReactorBus();

        reactorBus.forEventType(B.class).subscribe(b -> {
            System.out.println("b start");
            a.forEach(aa -> reactorBus.publish(new A(aa)));
            System.out.println("b end");
            System.out.println(b.data);
        });

        reactorBus.forEventType(A.class).subscribe(System.out::println);

        reactorBus.forEventType(Event.class).subscribe(e -> System.out.println("generic event " + e));

        reactorBus.forEventType(Event.class).filter(i -> i instanceof A).take(1).subscribe(h -> System.out.println("im gonna die now "+ h));

        reactorBus.publish(new B("n"));
    }

    @Data
    @AllArgsConstructor
    public static class A extends Event {
        String data;
    }

    @Data
    @AllArgsConstructor
    public static class B extends Event {
        String data;
    }
}
