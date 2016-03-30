package com.kunyan.userportrait.model

/**
  * Created by yangshuai on 2016/3/1.
  */
class EleItem {

  private var _name = ""

  private var _address = ""

  private var _phone = ""

  private var _totalName = ""

  private var _totalAddress = ""

  private var _totalPhone = ""


  def name_= (name: String): Unit = _name = name

  def address_= (address: String): Unit = _address = address

  def phone_= (phone: String): Unit = _phone = phone

  def totalName_= (totalName: String): Unit = _totalName = totalName

  def totalAddress_= (totalAddress: String): Unit = _totalAddress = totalAddress

  def totalPhone_= (totalPhone: String): Unit = _totalPhone = totalPhone


  def name = _name

  def address = _address

  def phone = _phone

  def totalName = _totalName

  def totalAddress = _totalAddress

  def totalPhone = _totalPhone

}
