package com.kawamind.kawfka;

import com.kawamind.kawfka.admin.ListTopics;
import com.kawamind.kawfka.config.Configuration;
import com.kawamind.kawfka.producer.KawaProducer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

import java.io.IOException;

import static picocli.CommandLine.Option;
import static picocli.CommandLine.ParseResult;

@Command(subcommands = { KawaProducer.class, ListTopics.class }, name = "kawfka",
        description = "kafka swiss army knife")
public class Kawfka implements Runnable {

    @Option(names = "--help", usageHelp = true, description = "display a help message")
    private final boolean helpRequested = false;

    @Option(names = { "-e", "--env" })
    public String environement = "";

    @Option(names = { "-c", "--configFile" })
    String configFile = null;

    @Spec
    CommandSpec spec;

    public static void main(String[] args) {
        com.kawamind.kawfka.Kawfka kawfka = new com.kawamind.kawfka.Kawfka();
        int exitCode = new CommandLine(kawfka).setExecutionStrategy(kawfka::executionStrategy).execute(args);
    }

    public void run() {
        throw new ParameterException(spec.commandLine(), "Missing required subcommand");
    }

    private int executionStrategy(ParseResult parseResult) {
        try {
            Configuration.initConfiguration(configFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new CommandLine.RunLast().execute(parseResult);
    }

}
