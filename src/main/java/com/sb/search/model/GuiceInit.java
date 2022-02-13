package com.sb.search.model;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class GuiceInit {

    private final Injector injector;

    /**
     *
     * @param entryPoint is the entry point in the har class, it needs a module that depends
     *                   on the aws dynamo,s3 configs and databricks configs
     */
    public GuiceInit(String entryPoint)   {
        injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {

            }
        }, new ModelModule(entryPoint));
    }

    public Injector getInjector(){
        return injector;
    }
}
