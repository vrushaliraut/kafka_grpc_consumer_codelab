package com.vrushali.marvel;

import com.example.grpc.Marvel.MarvelSuperheroRequest;
import com.example.grpc.Marvel.MarvelSuperheroResponse;
import com.example.grpc.MarvelSuperHeroServiceGrpc;
import com.vrushali.marvel.repository.KafkaRepository;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class Client {

    private static final int CLIENT_PORT = 8080;
    private static final String CLIENT_LHOST = "localhost";
    private ManagedChannel channel;
    private final MarvelSuperHeroServiceGrpc.MarvelSuperHeroServiceBlockingStub blockingStub;
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Client(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build());
    }

    Client(ManagedChannel channel) {
        blockingStub = MarvelSuperHeroServiceGrpc.newBlockingStub(channel);
        this.channel = channel;
    }

    private void shutdown() throws InterruptedException {
        channel.shutdown()
                .awaitTermination(5, TimeUnit.SECONDS);
    }

    void addMarvelSuperhero(String superhero_name) {
        MarvelSuperheroRequest request = MarvelSuperheroRequest
                .newBuilder()
                .setSuperheroName(superhero_name)
                .build();
        MarvelSuperheroResponse response;
        try {
            response = blockingStub.addMarvelSuperHero(request);
            System.out.println("response got from server : " + response.getSuperheroName());
        } catch (RuntimeException e) {
            logger.debug("error: ", e);
        }
    }

    public static void main(String[] args) throws InterruptedException {

        String TOPIC = "test";
        String groupId = "consumer-tutorial-group";

        KafkaRepository kafkaRepository = new KafkaRepository(groupId, Collections.singletonList(TOPIC));
        String message = kafkaRepository.consumeMessage();

        Client client = new Client(CLIENT_LHOST, CLIENT_PORT);
        try {
            client.addMarvelSuperhero(message);
        } finally {
            client.shutdown();
        }
    }
}
