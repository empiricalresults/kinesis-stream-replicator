package er.replicator;


import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Import(Beans.class)
@ComponentScan
@EnableAutoConfiguration
@EnableConfigurationProperties(Config.class)
public class Main {


    private static final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Autowired
    Worker worker;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @PostConstruct
    public void init() {
        executor.submit(worker);
    }
}
