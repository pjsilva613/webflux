package com.springboot.webflux.app.controllers;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.thymeleaf.spring6.context.webflux.ReactiveDataDriverContextVariable;

import com.springboot.webflux.app.models.daos.ProductoDao;
import com.springboot.webflux.app.models.documents.Producto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/products")
public class ProductRestController {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProductRestController.class);
	@Autowired
	private ProductoDao productoDao;

	@GetMapping({"/listar", "/"})
	public Flux<Producto> listar(Model model) {
		Flux<Producto> products = productoDao.findAll().map(product ->{
			product.setNombre(product.getNombre().toUpperCase());
			return product;
		});
		products.subscribe(p -> LOGGER.info(p.toString()));
		return products;
	}
	
	@GetMapping("/{id}")
	public Mono<Producto> buscarPorId(@PathVariable("id") String id) {
		//forma 1
		/*Mono<Producto> producto = productoDao.findById(id).map(product ->{
			product.setNombre(product.getNombre().toUpperCase());
			return product;
		});
		return producto;*/
		Mono<Producto> product = productoDao.findAll().filter(p -> p.getId().equals(id)).next();
		return product;
	}
	
}
