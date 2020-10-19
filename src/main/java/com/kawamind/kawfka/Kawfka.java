package com.kawamind.kawfka;

import com.kawamind.kawfka.admin.Topics;
import com.kawamind.kawfka.io.consumer.KawfkaConsumer;
import com.kawamind.kawfka.io.producer.KawfkaProducer;
import io.quarkus.picocli.runtime.PicocliCommandLineFactory;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import javax.inject.Inject;

@QuarkusMain
@TopCommand
@Command(subcommands = {KawfkaProducer.class, Topics.class, KawfkaConsumer.class}, name = "kawfka",
        description = "kafka swiss army knife", mixinStandardHelpOptions = true)
public class Kawfka implements Runnable, QuarkusApplication {

    @Spec
    CommandSpec spec;

    @Inject
    PicocliCommandLineFactory commandLineFactory;

    @Override
    public int run(final String... args) throws Exception {
        return commandLineFactory.create().execute(args);
    }

    public void run() {
        commandLineFactory.create().execute("-h");
        //throw new ParameterException(spec.commandLine(), "Missing required subcommand");
    }
}
