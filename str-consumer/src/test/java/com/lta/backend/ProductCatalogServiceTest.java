package com.lta.backend;
import com.lta.backend.services.ProductCatalogService;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class ProductCatalogServiceTest {
    private final ProductCatalogService catalog = new ProductCatalogService();
    @Test void searchMatchesAccentsCaseIdAndCategory() {
        assertEquals(6, catalog.search("").size());
        assertEquals(1, catalog.search("  BÓWL  ").size());
        assertEquals("PRD-004", catalog.search("prd-004").get(0).id());
        assertEquals(2, catalog.search("vegano").size());
        assertTrue(catalog.search("zzzzz").isEmpty());
    }
}
