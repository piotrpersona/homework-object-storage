package gateway

import (
	"fmt"
	"log"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/spacelift-io/homework-object-storage/internal/storage"
)

func NewServer(s storage.Storage) (e *echo.Echo) {
	e = echo.New()
	e.Use(middleware.Recover())
	e.PUT("/object/:id", putObject(s))
	e.GET("/object/:id", getObject(s))
	return
}

type Response struct {
	Message string
}

func validateObjectID(id string) bool {
	return len(id) > 0 && len(id) < 33
}

func putObject(storage storage.Storage) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		objectID := c.Param("id")
		if !validateObjectID(objectID) {
			return c.JSON(http.StatusBadRequest, Response{Message: "provide objectID of length [1,32]"})
		}
		err := storage.Put(ctx, objectID, []byte("example"))
		if err != nil {
			log.Printf("cannot put object, err: %s", err)
			return c.JSON(http.StatusInternalServerError, Response{Message: fmt.Sprintf("cannot put object '%s'", objectID)})
		}
		return c.JSON(http.StatusOK, Response{Message: fmt.Sprintf("object '%s' was successfully put", objectID)})
	}
}

func getObject(storage storage.Storage) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		objectID := c.Param("id")
		if !validateObjectID(objectID) {
			return c.JSON(http.StatusBadRequest, Response{Message: "provide objectID of length [1,32]"})
		}
		bytes, err := storage.Get(ctx, objectID)
		if err != nil {
			log.Printf("cannot get object, err: %s", err)
			return c.JSON(http.StatusInternalServerError, Response{Message: fmt.Sprintf("cannot get object '%s'", objectID)})
		}
		return c.String(http.StatusOK, string(bytes))
	}
}
