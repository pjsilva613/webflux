package com.springboot.reactor.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.springboot.reactor.app.models.Usuario;

import reactor.core.publisher.Flux;

@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SpringBootReactorApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringBootReactorApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		Flux<Usuario> nombres = Flux.just("Pedro fulano","juan sutano", "luis fularno","maria sutana", "bruce willis", "bruce lee")
				.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.filter(usuario -> usuario.getNombres().equalsIgnoreCase("bruce"))
				.doOnNext(usuario ->{
			if (usuario.getNombres().isEmpty()) {
				throw new RuntimeException("El nombre no puede ser vacio");
			}
			LOGGER.info(usuario.toString());
		}).map(usuario -> { 
			String nuevoNombre = usuario.getNombres().toLowerCase();		
			usuario.setNombres(nuevoNombre);
			return usuario;});
		
		nombres.subscribe(e -> LOGGER.info(e.toString()), error -> LOGGER.error(error.getMessage()), new Runnable() {
			
			@Override
			public void run() {
				LOGGER.info("Ha finalizado el flujo de datos con exito");				
			}
		});
		
	}

}
