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

type ResponseMessage struct {
	Message string
}

func putObject(storage storage.Storage) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		objectID := "object"
		err := storage.Put(ctx, objectID, []byte("example"))
		if err != nil {
			log.Printf("gannot put object, err: %s", err)
			return c.JSON(http.StatusInternalServerError, ResponseMessage{Message: fmt.Sprintf("cannot put object '%s'", objectID)})
		}
		return c.JSON(http.StatusOK, ResponseMessage{Message: fmt.Sprintf("object '%s' was successfully put", objectID)})
	}
}

func getObject(storage storage.Storage) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx := c.Request().Context()
		objectID := "object"
		bytes, err := storage.Get(ctx, objectID)
		if err != nil {
			log.Printf("gannot get object, err: %s", err)
			return c.JSON(http.StatusInternalServerError, ResponseMessage{Message: fmt.Sprintf("cannot get object '%s'", objectID)})
		}
		return c.Blob(http.StatusOK, "text/plain", bytes)
	}
}
