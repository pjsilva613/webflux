package com.springboot.reactor.app;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.springboot.reactor.app.models.Comentarios;
import com.springboot.reactor.app.models.Usuario;
import com.springboot.reactor.app.models.UsuarioComentarios;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(SpringBootReactorApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringBootReactorApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		ejemploFlatMap();
	}
	
	
	public void ejemploFlatMap() throws Exception {

		List<String> usuariosList = new ArrayList<>();
		usuariosList.add("Andres Guzman");
		usuariosList.add("Pedro Fulano");
		usuariosList.add("Maria Fulana");
		usuariosList.add("Diego Sultano");
		usuariosList.add("Juan Mengano");
		usuariosList.add("Bruce Lee");
		usuariosList.add("Bruce Willis");

		Flux.fromIterable(usuariosList)
				.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.flatMap(usuario -> {
					if (usuario.getNombre().equalsIgnoreCase("bruce")) {
						return Mono.just(usuario);
					} else {
						return Mono.empty();
					}
				}).map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				}).subscribe(u -> LOGGER.info(u.toString()));
	}

	public void ejemploIterable() throws Exception {

		List<String> usuariosList = new ArrayList<>();
		usuariosList.add("Andres Guzman");
		usuariosList.add("Pedro Fulano");
		usuariosList.add("Maria Fulana");
		usuariosList.add("Diego Sultano");
		usuariosList.add("Juan Mengano");
		usuariosList.add("Bruce Lee");
		usuariosList.add("Bruce Willis");

		Flux<String> nombres = Flux
				.fromIterable(usuariosList); /*
												 * Flux.just("Andres Guzman" , "Pedro Fulano" , "Maria Fulana",
												 * "Diego Sultano", "Juan Mengano", "Bruce Lee", "Bruce Willis");
												 */

		Flux<Usuario> usuarios = nombres
				.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.filter(usuario -> usuario.getNombre().equalsIgnoreCase("bruce")).doOnNext(usuario -> {
					if (usuario == null) {
						throw new RuntimeException("Nombres no pueden ser vacíos");
					}

					System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
				}).map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});

		usuarios.subscribe(e -> LOGGER.info(e.toString()), error -> LOGGER.error(error.getMessage()), new Runnable() {

			@Override
			public void run() {
				LOGGER.info("Ha finalizado la ejecución del observable con éxito!");
			}
		});

	}

	private void bases() {
		Flux<Usuario> nombres = Flux
				.just("Pedro fulano", "juan sutano", "luis fularno", "maria sutana", "bruce willis", "bruce lee")
				.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.filter(usuario -> usuario.getNombre().equalsIgnoreCase("bruce")).doOnNext(usuario -> {
					if (usuario.getNombre().isEmpty()) {
						throw new RuntimeException("El nombre no puede ser vacio");
					}
					LOGGER.info(usuario.toString());
				}).map(usuario -> {
					String nuevoNombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nuevoNombre);
					return usuario;
				});

		nombres.subscribe(e -> LOGGER.info(e.toString()), error -> LOGGER.error(error.getMessage()), new Runnable() {

			@Override
			public void run() {
				LOGGER.info("Ha finalizado el flujo de datos con exito");
			}
		});

	}

	private void iterable() {
		List<String> usuariosList = new ArrayList<>();
		usuariosList.add("Andres Guzman");
		usuariosList.add("Pedro Fulano");
		usuariosList.add("Maria Fulana");
		usuariosList.add("Diego Sultano");
		usuariosList.add("Juan Mengano");
		usuariosList.add("Bruce Lee");
		usuariosList.add("Bruce Willis");

		Flux<String> nombres = Flux
				.fromIterable(usuariosList); /*
												 * Flux.just("Andres Guzman" , "Pedro Fulano" , "Maria Fulana",
												 * "Diego Sultano", "Juan Mengano", "Bruce Lee", "Bruce Willis");
												 */

		Flux<Usuario> usuarios = nombres
				.map(nombre -> new Usuario(nombre.split(" ")[0].toUpperCase(), nombre.split(" ")[1].toUpperCase()))
				.filter(usuario -> usuario.getNombre().equalsIgnoreCase("bruce")).doOnNext(usuario -> {
					if (usuario == null) {
						throw new RuntimeException("Nombres no pueden ser vacíos");
					}

					System.out.println(usuario.getNombre().concat(" ").concat(usuario.getApellido()));
				}).map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});

		usuarios.subscribe(e -> LOGGER.info(e.toString()), error -> LOGGER.error(error.getMessage()), new Runnable() {

			@Override
			public void run() {
				LOGGER.info("Ha finalizado la ejecución del observable con éxito!");
			}
		});

	}

}
