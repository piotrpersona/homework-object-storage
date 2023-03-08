package gateway

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func NewServer() (e *echo.Echo) {
	e = echo.New()
	e.Use(middleware.Recover())
	e.PUT("/object/{id}", putObject)
	e.GET("/object/{id}", getObject)
	return
}

func putObject(c echo.Context) (err error) {
	return
}

func getObject(c echo.Context) (err error) {
	return
}
