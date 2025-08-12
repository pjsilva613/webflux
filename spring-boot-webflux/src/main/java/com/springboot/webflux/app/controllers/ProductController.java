package com.springboot.webflux.app.controllers;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.thymeleaf.spring6.context.webflux.ReactiveDataDriverContextVariable;

import com.springboot.webflux.app.models.daos.ProductoDao;
import com.springboot.webflux.app.models.documents.Producto;

import reactor.core.publisher.Flux;

@Controller
public class ProductController {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProductController.class);
	@Autowired
	private ProductoDao productoDao;

	@GetMapping({"/listar", "/"})
	public String listar(Model model) {
		Flux<Producto> productos = productoDao.findAll().map(product ->{
			product.setNombre(product.getNombre().toUpperCase());
			return product;
		});
		productos.subscribe(p -> LOGGER.info(p.toString()));
		model.addAttribute( "productos",productos);
		model.addAttribute("titulo", "Listado de productos");
		return "listar";
	}
	
	@GetMapping("/listar-data-driver")
	public String listarDataDriver(Model model) {
		Flux<Producto> productos = productoDao.findAll().map(product ->{
			product.setNombre(product.getNombre().toUpperCase());
			return product;
		}).delayElements(Duration.ofSeconds(1));
		productos.subscribe(p -> LOGGER.info(p.toString()));
		model.addAttribute( "productos",productos);
		model.addAttribute("titulo", "Listado de productos");
		return "listar";
	}
	
	@GetMapping("/listar-data-driver-2")
	public String listarDataDriver2(Model model) {
		Flux<Producto> productos = productoDao.findAll().map(product ->{
			product.setNombre(product.getNombre().toUpperCase());
			return product;
		}).delayElements(Duration.ofSeconds(1));
		productos.subscribe(p -> LOGGER.info(p.toString()));
		model.addAttribute( "productos",new ReactiveDataDriverContextVariable(productos, 2));
		model.addAttribute("titulo", "Listado de productos");
		return "listar";
	}
	
	@GetMapping("/listar-full")
	public String listarFull(Model model) {
		Flux<Producto> productos = productoDao.findAll().map(product ->{
			product.setNombre(product.getNombre().toUpperCase());
			return product;
		}).repeat(5000);
		model.addAttribute( "productos",new ReactiveDataDriverContextVariable(productos, 2));
		model.addAttribute("titulo", "Listado de productos");
		return "listar";
	}
	
	@GetMapping("/listar-chuked")
	public String listarChunked(Model model) {
		Flux<Producto> productos = productoDao.findAll().map(product ->{
			product.setNombre(product.getNombre().toUpperCase());
			return product;
		}).repeat(5000);
		model.addAttribute( "productos",new ReactiveDataDriverContextVariable(productos, 2));
		model.addAttribute("titulo", "Listado de productos");
		return "listar-chunked";
	}
}
