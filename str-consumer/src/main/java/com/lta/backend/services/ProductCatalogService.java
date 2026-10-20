package com.lta.backend.services;

import java.math.BigDecimal;
import java.text.Normalizer;
import java.util.List;
import java.util.Locale;
import org.springframework.stereotype.Service;

/** Datos de demostración en memoria; no requiere base de datos. Precios en MXN. */
@Service
public class ProductCatalogService {
    public record Product(String id, String name, String category, BigDecimal price, int stock) {}
    private final List<Product> catalog = List.of(
        new Product("PRD-001", "Bowl de tofu", "Vegano", new BigDecimal("145.00"), 18),
        new Product("PRD-002", "Ensalada de quinoa", "Vegano", new BigDecimal("120.00"), 24),
        new Product("PRD-003", "Pasta al pesto", "Vegetariano", new BigDecimal("165.00"), 12),
        new Product("PRD-004", "Pizza margarita", "Vegetariano", new BigDecimal("195.00"), 8),
        new Product("PRD-005", "Tacos de res", "Carne", new BigDecimal("135.00"), 30),
        new Product("PRD-006", "Pollo al horno", "Carne", new BigDecimal("180.00"), 0)
    );

    public List<Product> search(String query) {
        String term = normalize(query.strip());
        return catalog.stream().filter(p -> normalize(p.id() + " " + p.name() + " " + p.category()).contains(term)).toList();
    }

    private String normalize(String text) {
        return Normalizer.normalize(text, Normalizer.Form.NFD)
            .replaceAll("\\p{M}", "").toLowerCase(Locale.ROOT);
    }
}
